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
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.QueryExecution;
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
    try {
      if (tsdb.getConfig().hasProperty("tsd.executor.plugin.guava.limit.objects")) {
        max_objects = tsdb.getConfig().getInt(
            "tsd.executor.plugin.guava.limit.objects");
      }
      if (tsdb.getConfig().hasProperty("tsd.executor.plugin.guava.limit.bytes")) {
        size_limit = tsdb.getConfig().getInt(
            "tsd.executor.plugin.guava.limit.bytes");
      }
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
    int i = 0;
    for (final byte[] key : keys) {
      System.out.println("            i: " + i);
      final int x = i;
      if (i++ == 2) {
        callback.onCacheResult(new CacheQueryResult() {

          @Override
          public byte[] key() {
            return key;
          }

          @Override
          public Map<String, QueryResult> results() {
            System.out.println(" RETURNING RESULTS!!!!!!!!!! at: " + x);
            try {
            Map<String, QueryResult> map = Maps.newHashMap();
            map.put("ds:m1", new RS());
            System.out.println("         here");
            return map;
            } catch (Throwable t) {
              t.printStackTrace();
            }
            return null;
          }
          
          class RS implements QueryResult {
            MockTimeSeries mts;
            RS() {
              mts = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
                  .setMetric("sys.if.in")
                  .addTags("host", "web02")
                  .addTags("dc", "lga")
                  .build());
              MutableNumericValue v = new MutableNumericValue(new SecondTimeStamp(1564086600), 42);
              mts.addValue(v);
            }
            
            @Override
            public TimeSpecification timeSpecification() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public Collection<TimeSeries> timeSeries() {
              return Lists.newArrayList(mts);
            }

            @Override
            public String error() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public Throwable exception() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public long sequenceId() {
              // TODO Auto-generated method stub
              return 0;
            }

            @Override
            public QueryNode source() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public String dataSource() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public TypeToken<? extends TimeSeriesId> idType() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public ChronoUnit resolution() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public RollupConfig rollupConfig() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public void close() {
              // TODO Auto-generated method stub
              
            }
            
          }
        });
        continue;
      }
      
      callback.onCacheResult(new CacheQueryResult() {

        @Override
        public byte[] key() {
          return key;
        }

        @Override
        public Map<String, QueryResult> results() {
          return null; // to simulte a cache miss. Empty to simulate a negative cache hit
        }
        
      });
    }
  }
  
  @Override
  public QueryExecution<byte[]> fetch(final QueryContext context, 
                                      final byte[] key,
                                      final Span upstream_span) {
    /** The execution we'll return. */
    class LocalExecution extends QueryExecution<byte[]> {
      public LocalExecution() {
        super(null);
        
//        if (context.getTracer() != null) {
//          setSpan(context, 
//              GuavaLRUCache.this.getClass().getSimpleName(), 
//              upstream_span,
//              TsdbTrace.addTags(
//                  "key", Bytes.pretty(key),
//                  "startThread", Thread.currentThread().getName()));
//        }
      }
      
      /** Do da work */
      void execute() {
        if (cache == null) {
          final IllegalStateException ex = 
              new IllegalStateException("Cache has not been initialized.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (key == null) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Key cannot be null.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (key.length < 1) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Key must be at least 1 byte long.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        
        final ByteArrayKey cache_key = new ByteArrayKey(key);
        final ExpiringValue value = cache.getIfPresent(cache_key);
        if (value == null) {
          callback(null, 
              TsdbTrace.successfulTags("cacheStatus","miss"));
          return;
        }
        if (value.expired()) {
          // Note: there is a race condition here where a call to cache() can write
          // an updated version of the same key with a newer expiration. Since this
          // isn't a full, solid implementation of an expiring cache yet, this is
          // a best-effort run and may invalidate new data.
          cache.invalidate(cache_key);
          expired.incrementAndGet();
          callback(null,
              TsdbTrace.successfulTags("cacheStatus","miss"));
          return;
        }
        callback(value.value,
            TsdbTrace.successfulTags(
                "cacheStatus","hit",
                "resultSize", value.value == null 
                  ? "0" : Integer.toString(value.value.length)));
      }

      @Override
      public void cancel() {
        // No-op.
      }
      
    }

    final LocalExecution execution = new LocalExecution();
    execution.execute();
    return execution;
  }

  @Override
  public QueryExecution<byte[][]> fetch(final QueryContext context, 
                                        final byte[][] keys,
                                        final Span upstream_span) {
    class LocalExecution extends QueryExecution<byte[][]> {
      public LocalExecution() {
        super(null);
        
        final StringBuilder buf = new StringBuilder();
        if (keys != null) {
          for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
              buf.append(", ");
            }
            buf.append(Bytes.pretty(keys[i]));
          }
        }
        
//        setSpan(context, 
//            GuavaLRUCache.this.getClass().getSimpleName(), 
//            upstream_span,
//            TsdbTrace.addTags(
//                "keys", buf.toString(),
//                "startThread", Thread.currentThread().getName()));
      }
      
      /** Do da work */
      void execute() {
        if (cache == null) {
          final IllegalStateException ex = 
              new IllegalStateException("Cache has not been initialized.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (keys == null) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Keys cannot be null.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        if (keys.length < 1) {
          final IllegalArgumentException ex = 
              new IllegalArgumentException("Keys must have at least 1 value.");
          callback(ex,
              TsdbTrace.exceptionTags(ex),
              TsdbTrace.exceptionAnnotation(ex));
          return;
        }
        final byte[][] results = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
          if (keys[i] == null) {
            final IllegalArgumentException ex = 
                new IllegalArgumentException("Key at index " + i + " was null.");
            callback(ex,
                TsdbTrace.exceptionTags(ex),
                TsdbTrace.exceptionAnnotation(ex));
            return;
          }
          final ByteArrayKey cache_key = new ByteArrayKey(keys[i]);
          final ExpiringValue value = cache.getIfPresent(cache_key);
          if (value != null) { 
            if (value.expired()) {
              // Note: there is a race condition here where a call to cache() can write
              // an updated version of the same key with a newer expiration. Since this
              // isn't a full, solid implementation of an expiring cache yet, this is
              // a best-effort run and may invalidate new data.
              cache.invalidate(cache_key);
              expired.incrementAndGet();
            } else {
              results[i] = value.value;
            }
          }
        }
        callback(results);
      }

      @Override
      public void cancel() {
        // No-op.
      }
      
    }
    
    final LocalExecution execution = new LocalExecution();
    execution.execute();
    return execution;
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
          new ExpiringValue(data[i], expirations[i], units));
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
  
  public class MockTimeSeries implements TimeSeries {

    /** The non-null ID. */
    protected final TimeSeriesStringId id;
    
    /** Whether or not we should sort when returning iterators. */
    protected final boolean sort;
    
    /** Whether or not closed has been called. */
    protected boolean closed;
    
    /** The map of types to lists of time series. */
    protected Map<TypeToken<? extends TimeSeriesDataType>, 
      List<TimeSeriesValue<?>>> data;
    
    /**
     * Default ctor.
     * @param id A non-null Id.
     */
    public MockTimeSeries(final TimeSeriesStringId id) {
      this(id, false);
    }
    
    /**
     * Alternate ctor to set sorting.
     * @param id A non-null Id.
     * @param sort Whether or not to sort on timestamps on the output.
     */
    public MockTimeSeries(final TimeSeriesStringId id, final boolean sort) {
      this.sort = sort;
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      this.id = id;
      data = Maps.newHashMap();
    }
    
    /**
     * @param value A non-null value to add to the proper array. Must return a type.
     */
    public void addValue(final TimeSeriesValue<?> value) {
      if (value == null) {
        throw new IllegalArgumentException("Can't store null values.");
      }
      List<TimeSeriesValue<?>> types = data.get(value.type());
      if (types == null) {
        types = Lists.newArrayList();
        data.put(value.type(), types);
      }
      types.add(value);
    }
    
    /** Flushes the map of data but leaves the ID alone. Also resets 
     * the closed flag. */
    public void clear() {
      data.clear();
      closed = false;
    }
    
    @Override
    public TimeSeriesStringId id() {
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      List<TimeSeriesValue<? extends TimeSeriesDataType>> types = data.get(type);
      if (types == null) {
        return Optional.empty();
      }
      if (sort) {
        Collections.sort(types, new TimeSeriesValue.TimeSeriesValueComparator());
      }
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = new MockTimeSeriesIterator(types.iterator(), type);
      return Optional.of(it);
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators
        = Lists.newArrayListWithCapacity(data.size());
      for (final Entry<TypeToken<? extends TimeSeriesDataType>, 
          List<TimeSeriesValue<?>>> entry : data.entrySet()) {
        
        iterators.add(new MockTimeSeriesIterator(entry.getValue().iterator(), entry.getKey()));
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return data.keySet();
    }

    @Override
    public void close() {
      closed = true;
    }

    public boolean closed() {
      return closed;
    }
    
    public Map<TypeToken<? extends TimeSeriesDataType>, 
        List<TimeSeriesValue<?>>> data() {
      return data;
    }
    
    /**
     * Iterator over the list of values.
     */
    class MockTimeSeriesIterator implements TypedTimeSeriesIterator {
      private final Iterator<TimeSeriesValue<?>> iterator;
      private final TypeToken<? extends TimeSeriesDataType> type;
      
      MockTimeSeriesIterator(final Iterator<TimeSeriesValue<?>> iterator,
                             final TypeToken<? extends TimeSeriesDataType> type) {
        this.iterator = iterator;
        this.type = type;
      }
      
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesValue<?> next() {
        return iterator.next();
      }
      
      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return type;
      }
      
    }
    
  }
}
