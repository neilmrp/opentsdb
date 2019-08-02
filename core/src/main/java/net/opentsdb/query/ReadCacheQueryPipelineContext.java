package net.opentsdb.query;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryPipelineContext.ResultWrapper;
import net.opentsdb.query.execution.cache.CachingSemanticQueryContext;
import net.opentsdb.query.execution.cache.CombinedArray;
import net.opentsdb.query.execution.cache.CombinedNumeric;
import net.opentsdb.query.execution.cache.CombinedSummary;
import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.QueryCachePlugin.CacheCB;
import net.opentsdb.query.execution.cache.QueryCachePlugin.CacheQueryResult;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

public class ReadCacheQueryPipelineContext extends AbstractQueryPipelineContext implements CacheCB {
  private static final Logger LOG = LoggerFactory.getLogger(ReadCacheQueryPipelineContext.class);
  
  public static final String CACHE_PLUGIN_KEY = "tsd.query.cache.plugin_id";
  public static final String KEYGEN_PLUGIN_KEY = "tsd.query.cache.keygen.plugin_id";
  
  protected int[] slices;
  protected int interval_in_seconds;
  protected QueryCachePlugin cache;
  protected TimeSeriesCacheKeyGenerator key_gen;
  protected boolean tip_query;
  protected byte[][] keys;
  protected MissOrTip[] fills;
  protected AtomicInteger latch;
  
  ReadCacheQueryPipelineContext(final QueryContext context, 
                                final List<QuerySink> direct_sinks) {
    super(context);
    if (direct_sinks != null && !direct_sinks.isEmpty()) {
      sinks.addAll(direct_sinks);
    }
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    registerConfigs(context.tsdb());
    
    cache = context.tsdb().getRegistry().getPlugin(QueryCachePlugin.class, 
        context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY));
    if (cache == null) {
      throw new IllegalArgumentException("No cache plugin found for: " +
          (Strings.isNullOrEmpty(
              context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY)) ? 
                  "Default" : context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY)));
    }
    key_gen = context.tsdb().getRegistry()
        .getPlugin(TimeSeriesCacheKeyGenerator.class, 
            context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY));
    if (key_gen == null) {
      throw new IllegalArgumentException("No key gen plugin found for: " + 
          (Strings.isNullOrEmpty(
              context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY)) ? 
                  "Default" : context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY)));
    }
    
    // TODO - pull this out into another shared function.
    // For now we find the highest common denominator for intervals.
    
    // TODO - issue: If we have a downsample of 1w, we can't query on 1 day segments
    // so we either cache the whole shebang or we bypass the cache.
    interval_in_seconds = 0;
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      final DownsampleFactory factory = context.tsdb().getRegistry()
          .getDefaultPlugin(DownsampleFactory.class);
      if (config instanceof DownsampleConfig) {
        String interval;
        if (((DownsampleConfig) config).getRunAll()) {
          final long delta = context.query().endTime().epoch() - 
              context.query().startTime().epoch();
          // TODO - rollup config and we need to tweak the downsampler to handle
          // this case as well where we may want to use rollup data for really bit
          // queries.
          if (delta >= 86400) {
            interval = "1d";
          } else {
            interval = "1m";
          }
        } else if (((DownsampleConfig) config).getOriginalInterval()
            .toLowerCase().equals("auto")) {
          final long delta = context.query().endTime().msEpoch() - 
              context.query().startTime().msEpoch();
          interval = DownsampleFactory.getAutoInterval(delta, factory.intervals());
          System.out.println("       AUTO: " + interval);
        } else {
          // normal interval
          interval = ((DownsampleConfig) config).getInterval();
        }
        int parsed = (int) DateTime.parseDuration(interval) / 1000;
        System.out.println("final interval: " + interval + "  Parsed: " + parsed);
        interval_in_seconds = parsed;
      }
    }
    
    // TODO - in the future use rollup config. For now snap to one day.
    if (interval_in_seconds >= 3600) {
      interval_in_seconds = 86400;
    } else {
      interval_in_seconds = 3600;
    }
    
    // TODO - validate calendaring. May need to snap differently based on timezone.
    long start = context.query().startTime().epoch();
    start = start - (start % interval_in_seconds);
    
    long end = context.query().endTime().epoch();
    end = end - (end % interval_in_seconds);
    if (end != context.query().endTime().epoch()) {
      end += interval_in_seconds;
    }
    
    slices = new int[(int) ((end - start) / interval_in_seconds)];
    int ts = (int) start;
    for (int i = 0; i < slices.length; i++) {
      slices[i] = ts;
      ts += interval_in_seconds;
    }
    
    keys = key_gen.generate(context.query().buildHashCode().asLong(), slices);
    if (tip_query) {
      keys = Arrays.copyOf(keys, keys.length - 1);
    }
    fills = new MissOrTip[slices.length];
    System.out.println("SLICES: " + Arrays.toString(slices));
    
    if (context.sinkConfigs() != null) {
      for (final QuerySinkConfig config : context.sinkConfigs()) {
        final QuerySinkFactory factory = context.tsdb().getRegistry()
            .getPlugin(QuerySinkFactory.class, config.getId());
        if (factory == null) {
          throw new IllegalArgumentException("No sink factory found for: " 
              + config.getId());
        }
        
        final QuerySink sink = factory.newSink(context, config);
        if (sink == null) {
          throw new IllegalArgumentException("Factory returned a null sink for: " 
              + config.getId());
        }
        sinks.add(sink);
        if (sinks.size() > 1) {
          throw new UnsupportedOperationException("Only one sink allowed for now, sorry!");
        }
      }
    }
    return Deferred.fromResult(null);
  }

  @Override
  public void fetchNext(final Span span) {
    LOG.info("WARNING: USING CACHE SMQE");
    // TODO - timeless
    if (tip_query) {
      latch = new AtomicInteger(keys.length);
    } else {
      latch = new AtomicInteger(keys.length);
    }
    
    cache.fetch(this, keys, this, null);
    
    if (tip_query) {
      fills[fills.length - 1] = new MissOrTip();
      fills[fills.length - 1].key = keys[keys.length - 1];
      fills[fills.length - 1].sub_context = buildQuery(slices[slices.length - 1], fills[fills.length - 1]);
      fills[fills.length - 1].sub_context.initialize(null)
        .addCallback(new FillCB(fills[fills.length - 1].sub_context))
        .addErrback(new ErrorCB());
    }
  }
  
  @Override
  public void onCacheResult(final CacheQueryResult result) {
    try {
    MissOrTip mot = null;
    int idx = 0;
    for (int i = 0; i < keys.length; i++) {
      if (Bytes.memcmp(keys[i], result.key()) == 0) {
        fills[i] = new MissOrTip();
        fills[i].key = result.key();
        fills[i].map = result.results();
        mot = fills[i];
        idx = i;
        break;
      }
    }
    
    if (mot.map == null) {
      mot.sub_context = buildQuery(slices[idx], mot);
      mot.sub_context.initialize(null)
         .addCallback(new FillCB(mot.sub_context))
         .addErrback(new ErrorCB());
    } else if (latch.decrementAndGet() == 0) {
      System.out.println("---------- CACHE RESULT!!!");
      run();
    } else {
      System.out.println("        CACHE HIT!!!!!!!: " + idx + "   latch: " + latch.get());
    }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Override
  public void onCacheError(Throwable t) {
    // TODO Auto-generated method stub
    
  }
  
  // MERGE AND SEND!
  public void run() {
    try {
    // sort and merge
    Map<String, QueryResult[]> sorted = Maps.newHashMap();
    for (int i = 0; i < fills.length; i++) {
      for (final Entry<String, QueryResult> entry : fills[i].map.entrySet()) {
        QueryResult[] qrs = sorted.get(entry.getKey());
        if (qrs == null) {
          qrs = new QueryResult[fills.length + (tip_query ? 1 : 0)];
          sorted.put(entry.getKey(), qrs);
        }
        qrs[i] = entry.getValue();
      }
    }
    
    // TODO tip
    latch.set(sorted.size());
    for (final QueryResult[] results : sorted.values()) {
      // TODO - implement
      // TODO - send in thread pool
      for (final QuerySink sink : sinks) {
        sink.onNext(new CombinedResult(results));
      }
    }
    
    for (int i = 0; i < fills.length; i++) {
      final int x = i;
      if (fills[i].sub_context == null) {
        continue;
      }
      
      // write to the cache
      // TODO - mode
      context.tsdb().getQueryThreadPool().submit(new Runnable() {
        @Override
        public void run() {
          cache.cache(0, keys[x], fills[x].map.values());
        }
      }, context);
    }
    
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  class MissOrTip implements QuerySink {
    byte[] key;
    QueryContext sub_context;
    volatile Map<String, QueryResult> map = Maps.newConcurrentMap();
    AtomicBoolean complete = new AtomicBoolean();
    
    @Override
    public void onComplete() {
      complete.compareAndSet(false, true);
      if (latch.decrementAndGet() == 0) {
        System.out.println("[[[[[[[[ COMPLETE!!! ]]]]]]] RUNNING");
        run();
      }
    }
    
    @Override
    public void onNext(QueryResult next) {
      final String id = next.source().config().getId() + ":" + next.dataSource();
      if (map == null) {
        synchronized (this) {
          if (map == null) {
            map = Maps.newConcurrentMap();
          }
        }
      }
      map.put(id, next);
      if (next instanceof ResultWrapper) {
        ((ResultWrapper) next).closeWrapperOnly();
      }
    }
    
    @Override
    public void onNext(PartialTimeSeries next, QuerySinkCallback callback) {
      // TODO Auto-generated method stub
      
    }
    
    @Override
    public void onError(Throwable t) {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  class CombinedTimeSeries implements TimeSeries {
    List<TimeSeries> series;
    
    CombinedTimeSeries(final TimeSeries ts) {
      System.out.println("NEW TIMESERIES: " + ts.id());
      series = Lists.newArrayList();
      series.add(ts);
    }
    
    @Override
    public TimeSeriesId id() {
      return series.get(0).id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (series.get(0).types().contains(type)) {
//        if (type == NumericType.TYPE) {
//          return Optional.of(new CombinedNumeric(series));
//        } else if (type == NumericArrayType.TYPE) {
//          return Optional.of(new CombinedArray(series));
//        } else if (type == NumericSummaryType.TYPE) {
//          return Optional.of(new CombinedSummary(series));
//        }
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
          Lists.newArrayList();
      TypeToken<? extends TimeSeriesDataType> type = series.get(0).types().iterator().next();
//      if (type == NumericType.TYPE) {
//        iterators.add(new CombinedNumeric(series));
//      } else if (type == NumericArrayType.TYPE) {
//        iterators.add(new CombinedArray(series));
//      } else if (type == NumericSummaryType.TYPE) {
//        iterators.add(new CombinedSummary(series));
//      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return series.get(0).types();
    }

    @Override
    public void close() {
//      for (final TimeSeries ts : series) {
//        ts.close();
//      }
    }
    
  }
  
  class CombinedResult implements QueryResult, TimeSpecification {

    Map<Long, TimeSeries> time_series;
    TimeStamp spec_start;
    TimeSpecification spec;
    QueryNode node;
    String data_source;
    
    CombinedResult(final QueryResult[] results) {
      time_series = Maps.newHashMap();
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          continue;
        }
        
        if (spec_start == null && results[i].timeSpecification() != null) {
          spec_start = results[i].timeSpecification().start();
        }
        
        node = results[i].source();
        data_source = results[i].dataSource();
        
        // TODO more time spec
        spec = results[i].timeSpecification();
        
        // TODO handle tip merge eventually
        for (final TimeSeries ts : results[i].timeSeries()) {
          final long hash = ts.id().buildHashCode();
          System.out.println("      ID HASH: " + hash);
          TimeSeries combined = time_series.get(hash);
          if (combined == null) {
            combined = new CombinedTimeSeries(ts);
            time_series.put(hash, combined);
          } else {
            ((CombinedTimeSeries) combined).series.add(ts);
          }
        }
      }
      System.out.println("        TOTAL RESULTS: " + time_series.size());
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return spec_start == null ? null : this;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return time_series.values();
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
      return node;
    }

    @Override
    public String dataSource() {
      return data_source;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public ChronoUnit resolution() {
      return ChronoUnit.SECONDS;
    }

    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      System.out.println("-------- CLOSING");
      if (latch.decrementAndGet() == 0) {
        for (final QuerySink sink : sinks) {
          sink.onComplete();
        }
      }
//      for (final TimeSeries ts : time_series.values()) {
//        ts.close();
//      }
    }

    @Override
    public TimeStamp start() {
      return spec_start;
    }

    @Override
    public TimeStamp end() {
      return spec.end();
    }

    @Override
    public TemporalAmount interval() {
      return spec.interval();
    }

    @Override
    public String stringInterval() {
      return spec.stringInterval();
    }

    @Override
    public ChronoUnit units() {
      return spec.units();
    }

    @Override
    public ZoneId timezone() {
      return spec.timezone();
    }

    @Override
    public void updateTimestamp(int offset, TimeStamp timestamp) {
      spec.updateTimestamp(offset, timestamp);
    }

    @Override
    public void nextTimestamp(TimeStamp timestamp) {
      spec.nextTimestamp(timestamp);
    }
    
  }

  QueryContext buildQuery(final int start, final QuerySink sink) {
    SemanticQuery.Builder builder = ((SemanticQuery) context.query()).toBuilder();
    builder.setStart(Integer.toString(start));
    int end = start + interval_in_seconds;
    if (end < context.query().endTime().epoch()) {
      // not tip so set it.
      builder.setEnd(Integer.toString(end));
    }
    
    return SemanticQueryContext.newBuilder()
        .setTSDB(context.tsdb())
        .setQuery(builder.build())
        .setStats(context.stats())
        .setAuthState(context.authState())
        .addSink(sink)
        .build();
  }

  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(CACHE_PLUGIN_KEY)) {
      tsdb.getConfig().register(CACHE_PLUGIN_KEY, null, true, 
          "The ID of a cache plugin to use.");
    }
    if (!tsdb.getConfig().hasProperty(KEYGEN_PLUGIN_KEY)) {
      tsdb.getConfig().register(KEYGEN_PLUGIN_KEY, null, true,
          "The ID of a key generator plugin to use.");
    }
  }
  
  static class FillCB implements Callback<Void, Void> {
    final QueryContext context;
    
    FillCB(final QueryContext context) {
      this.context = context;
    }
    
    @Override
    public Void call(final Void arg) throws Exception {
      context.fetchNext(null);
      return null;
    }
    
  }
  
  static class ErrorCB implements Callback<Void, Exception> {

    @Override
    public Void call(final Exception e) throws Exception {
      LOG.error("WTF?", e);
      return null;
    }
    
  }
}
