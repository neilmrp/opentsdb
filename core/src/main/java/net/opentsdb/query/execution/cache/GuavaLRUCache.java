// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.execution.cache;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.cache.QueryCachePlugin;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdes;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdesFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.Bytes.ByteArrayKey;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

/**
 * A very simple and basic implementation of an on-heap, in-memory LRU cache 
 * using the Guava {@link Cache} class for configurable size and thread safety.
 * <p>
 * This implementation wraps byte array keys in a {@code ByteArrayKey} for 
 * proper use in Guava's map implementation (otherwise we'd be comparing the
 * byte array addresses and that's no use to us!). It also wraps all of the
 * values in a tiny class that captures the insert timestamp and an expiration
 * time. In this manner we can imitate an expiring cache as well in that if a
 * value is queried after it's computed expiration time, it's kicked out of
 * the cache.
 * <b>Note:</b> This implementation is super basic so there is a race condition
 * during read and write that may blow out valid cache objects when:
 * <ol>
 * <li>A thread performs a lookup and finds that the object has expired.</li>
 * <li>Another thread writes a new cache object with the new expiration.<li>
 * <li>The first thread calls {@link Cache#invalidate(Object)} with the key
 * and the new object is deleted.</li>
 * </ol>
 * This shouldn't happen too often for small installs and for bigger, distributed
 * installs, users should use a distributed cache instead.
 * <p>
 * Also note that the cache attempts to track the number of actual bytes of
 * values in the store (doesn't include Guava overhead, the keys or the 8 bytes
 * of expiration timestamp). Some objects will NOT be cached if the size is too
 * large. Guava will kick out some objects an invalidation, the size counter 
 * will be decremented, allowing the next cache call to hopefully write some 
 * data.
 * <p>
 * Also note that this version allows for null values and empty values. Keys
 * may not be null or empty though.
 * 
 * @since 3.0
 */
public class GuavaLRUCache extends BaseTSDBPlugin implements 
    QueryCachePlugin, TimerTask {
  public static final String TYPE = GuavaLRUCache.class.getSimpleName().toString();
  private static final Logger LOG = LoggerFactory.getLogger(GuavaLRUCache.class);
  
  public static final String KEY_PREFIX = "tsd.cache.lru.";
  public static final String OBJECTS_LIMIT_KEY = "limit.objects";
  public static final String SIZE_LIMIT_KEY = "limit.size";
  public static final String SERDES_KEY = "serdes.id";
  
  /** The default size limit in bytes. 128MB. */
  public static final long DEFAULT_SIZE_LIMIT = 134217728;
  
  /** Default number of objects to maintain in the cache. */
  public static final int DEFAULT_MAX_OBJECTS = 1024;
  
  /** A counter used to track how many butes are in the cache. */
  private final AtomicLong size;
  
  /** A counter to track how many values have been expired out of the cache. */
  private final AtomicLong expired;
  
  /** Reference to the TSDB used for metrics. */
  private TSDB tsdb;
  
  /** The Guava cache implementation. */
  private Cache<ByteArrayKey, ExpiringValue> cache;
  
  /** The configured sized limit. */
  private long size_limit;
  
  /** The configured maximum number of objects. */
  private int max_objects; 
  
  /** The serdes plugin for this cache instance. */
  private TimeSeriesCacheSerdes serdes;
  
  /**
   * Default ctor.
   */
  public GuavaLRUCache() {
    size = new AtomicLong();
    expired = new AtomicLong();
    size_limit = DEFAULT_SIZE_LIMIT;
    max_objects = DEFAULT_MAX_OBJECTS;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerConfigs(tsdb);
    try {
      max_objects = tsdb.getConfig().getInt(getConfigKey(OBJECTS_LIMIT_KEY));
      size_limit = tsdb.getConfig().getInt(getConfigKey(SIZE_LIMIT_KEY));
      final String serdes_id = tsdb.getConfig().getString(getConfigKey(SERDES_KEY));
      final TimeSeriesCacheSerdesFactory factory = tsdb.getRegistry().getPlugin(
          TimeSeriesCacheSerdesFactory.class, serdes_id);
      if (factory == null) {
        return Deferred.fromError(new IllegalArgumentException("No serdes factory found for " +
            (Strings.isNullOrEmpty(serdes_id) ? "Default" : serdes_id)));
      }
      serdes = factory.getSerdes();
      cache = CacheBuilder.newBuilder()
          .maximumSize(max_objects)
          .removalListener(new Decrementer())
          .recordStats()
          .build();
      
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.<Object>fromResult(e);
    }
  }
  
  @Override
  public void fetch(final QueryPipelineContext context, 
      final byte[][] keys, 
      final CacheCB callback, 
      final Span upstream_span) {
    for (int i = 0; i < keys.length; i++) {
      final ByteArrayKey cache_key = new ByteArrayKey(keys[i]);
      ExpiringValue value = cache.getIfPresent(cache_key);
      if (value != null) { 
        if (value.expired()) {
          // Note: there is a race condition here where a call to cache() can write
          // an updated version of the same key with a newer expiration. Since this
          // isn't a full, solid implementation of an expiring cache yet, this is
          // a best-effort run and may invalidate new data.
          cache.invalidate(cache_key);
          expired.incrementAndGet();
          value = null;
        }
      }
      final ExpiringValue final_value = value;
      
      
      class CQR implements CacheQueryResults {
        final byte[] key;
        final Map<String, CachedQueryResult> results;
        CQR(final int idx) {
          key = keys[idx];
          if (final_value == null) {
            results = null;
          } else if (final_value.value == null) {
            results = Collections.emptyMap();
          } else {
            results = serdes.deserialize(final_value.value);
          }
        }
        
        @Override
        public byte[] key() {
          return key;
        }

        @Override
        public Map<String, CachedQueryResult> results() {
          return results;
        }

        @Override
        public TimeStamp lastValueTimestamp() {
          // TODO Auto-generated method stub
          return null;
        }
      }
      
      callback.onCacheResult(new CQR(i));
      
    }
  }
  
  @Override
  public Deferred<Void> cache(final int timestamp, 
                              final byte[] key, 
                              final long expiration,
                              final Collection<QueryResult> results) {
    System.out.println(" [[[[[[[[ CACHING ]]]]]]: " + results);
    cache(key, serdes.serialize(results), expiration, TimeUnit.MILLISECONDS, null);
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Void> cache(final int[] timestamps, 
                              final byte[][] keys,
                              final long[] expirations,
                              final Collection<QueryResult> results) {
    System.out.println(" [[[[[[[[ SPLITTING AND CACHING ]]]]]]: " + results);
    Arrays.fill(expirations, Long.MAX_VALUE);

    cache(keys, serdes.serialize(timestamps, keys, results), expirations, TimeUnit.MILLISECONDS, null);
    return null;
  }
  
  @Override
  public void cache(final byte[] key, 
                    final byte[] data, 
                    final long expiration, 
                    final TimeUnit units,
                    final Span upstream_span) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null.");
    }
    if (key.length < 1) {
      throw new IllegalArgumentException("Key length must be at least 1 byte.");
    }
    if (expiration < 1) {
      return;
    }

    // best effort
    if (size.get() + (data == null ? 0  : data.length) >= size_limit) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will not cache key [" + Bytes.pretty(key) 
          + "] due to size limit.");
      }
      return;
    }
    cache.put(new ByteArrayKey(key), new ExpiringValue(data, expiration, units));
    if (data != null) {
      size.addAndGet(data.length);
    }
  }

  @Override
  public void cache(final byte[][] keys, 
                    final byte[][] data, 
                    final long[] expirations,
                    final TimeUnit units,
                    final Span upstream_span) {
    if (cache == null) {
      throw new IllegalStateException("Cache has not been initialized.");
    }
    if (keys == null) {
      throw new IllegalArgumentException("Keys array cannot be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data array cannot be null.");
    }
    if (keys.length != data.length) {
      throw new IllegalArgumentException("Key and data arrays must be of the "
          + "same length.");
    }
    if (expirations == null) {
      throw new IllegalArgumentException("Expirations cannot be null.");
    }
    if (expirations.length != data.length) {
      throw new IllegalArgumentException("Expirations and data arrays must be "
          + "of the same length.");
    }
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        throw new IllegalArgumentException("Key at index " + i + " was null "
            + "and cannot be.");
      }
      if (expirations[i] < 1) {
        continue;
      }
      // best effort
      if (size.get() + (data == null ? 0  : data.length) >= size_limit) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will not cache key [" + Bytes.pretty(keys[i]) 
            + "] due to size limit.");
        }
        continue;
      }
      cache.put(new ByteArrayKey(keys[i]), 
          new ExpiringValue(data[i], Integer.MAX_VALUE/*expirations[i]*/, units));
      if (data[i] != null) {
        size.addAndGet(data[i].length);
      }
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  @VisibleForTesting
  Cache<ByteArrayKey, ExpiringValue> cache() {
    return cache;
  }
  
  @VisibleForTesting
  long bytesStored() {
    return size.get();
  }
  
  @VisibleForTesting
  long sizeLimit() {
    return size_limit;
  }
  
  @VisibleForTesting
  int maxObjects() {
    return max_objects;
  }
  
  @VisibleForTesting
  long expired() {
    return expired.get();
  }
  
  /** Super simple listener that decrements our size counter. */
  private class Decrementer implements 
      RemovalListener<ByteArrayKey, ExpiringValue> {
    @Override
    public void onRemoval(
        final RemovalNotification<ByteArrayKey, ExpiringValue> notification) {
      if (notification.getValue().value != null) {
        size.addAndGet(-notification.getValue().value.length);
      }
    }
  }
  
  /** Wrapper around a value that stores the expiration timestamp. */
  private class ExpiringValue {
    /** The value stored in the cache. */
    private final byte[] value;
    
    /** The expiration timestamp in unix epoch nanos. */
    private final long expires;
    
    /**
     * Default ctor.
     * @param value A value (may be null)
     * @param expiration The expiration value count in time units.
     * @param units The time units of the expiration.
     */
    public ExpiringValue(final byte[] value, 
                         final long expiration, 
                         final TimeUnit units) {
      this.value = value;
      switch (units) {
      case SECONDS:
        expires = DateTime.nanoTime() + (expiration * 1000 * 1000 * 1000);
        break;
      case MILLISECONDS:
        expires = DateTime.nanoTime() + (expiration * 1000 * 1000);
        break;
      case NANOSECONDS:
        expires = DateTime.nanoTime() + expiration;
        break;
      default:
        throw new IllegalArgumentException("Unsupported units: " + units);
      }
    }
    
    /** @return Whether or not the value has expired. */
    public boolean expired() {
      return DateTime.nanoTime() > expires;
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append("{value=")
          .append(Bytes.pretty(value))
          .append(", expires=")
          .append(expires)
          .toString();
    }
  }

  @Override
  public void run(final Timeout ignored) throws Exception {
    try {
      final CacheStats stats = cache.stats();
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.requestCount", 
          stats.requestCount(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.hitCount", 
          stats.hitCount(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.hitRate", 
          stats.hitRate(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.missCount", 
          stats.missCount(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.missRate", 
          stats.missRate(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.evictionCount", 
          stats.evictionCount(), (String[]) null);
      tsdb.getStatsCollector().setGauge("query.readCache.guava.lru.expiredCount", 
          expired.get(), (String[]) null);
    } catch (Exception e) {
      LOG.error("Unexpected exception recording LRU stats", e);
    }
    
    tsdb.getMaintenanceTimer().newTimeout(this, 
        tsdb.getConfig().getInt(DefaultTSDB.MAINT_TIMER_KEY), 
        TimeUnit.MILLISECONDS);
  }
  
  /**
   * Helper to build the config key with a factory id.
   *
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(OBJECTS_LIMIT_KEY))) {
      tsdb.getConfig().register(getConfigKey(OBJECTS_LIMIT_KEY), 1024, false, 
          "The maximum number of entries to have in the cache.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SIZE_LIMIT_KEY))) {
      tsdb.getConfig().register(getConfigKey(SIZE_LIMIT_KEY), 1024 * 16, false, 
          "The maximum number of bytes of data to main in the cache.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SERDES_KEY))) {
      tsdb.getConfig().register(getConfigKey(SERDES_KEY), null, false, 
          "The ID of a cache serdes plugin to load.");
    }
  }


  
}
