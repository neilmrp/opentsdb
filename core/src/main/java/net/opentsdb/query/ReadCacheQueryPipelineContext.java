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

public class ReadCacheQueryPipelineContext extends AbstractQueryPipelineContext 
    implements CacheCB {
  static final Logger LOG = LoggerFactory.getLogger(
      ReadCacheQueryPipelineContext.class);
  
  public static final String CACHE_PLUGIN_KEY = "tsd.query.cache.plugin_id";
  public static final String KEYGEN_PLUGIN_KEY = "tsd.query.cache.keygen.plugin_id";
  
  protected final long current_time;
  protected int[] slices;
  protected int interval_in_seconds;
  protected String string_interval;
  protected int min_interval;
  protected boolean skip_cache;
  protected QueryCachePlugin cache;
  protected TimeSeriesCacheKeyGenerator key_gen;
  protected boolean tip_query;
  protected byte[][] keys;
  protected ResultOrSubQuery[] results;
  protected AtomicInteger latch;
  protected AtomicInteger hits;
  protected AtomicBoolean failed;
  protected QueryContext sub_context;
  protected List<QueryResult> sub_results;
  
  ReadCacheQueryPipelineContext(final QueryContext context, 
                                final List<QuerySink> direct_sinks) {
    super(context);
    if (direct_sinks != null && !direct_sinks.isEmpty()) {
      sinks.addAll(direct_sinks);
    }
    failed = new AtomicBoolean();
    current_time = DateTime.currentTimeMillis();
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
    int ds_interval = Integer.MAX_VALUE;
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      final DownsampleFactory factory = context.tsdb().getRegistry()
          .getDefaultPlugin(DownsampleFactory.class);
      if (config instanceof DownsampleConfig) {
        String interval;
        if (((DownsampleConfig) config).getRunAll()) {
          skip_cache = true;
          break;
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
        if (parsed < ds_interval) {
          ds_interval = parsed;
        }
        System.out.println("          ds_interval: " + ds_interval + "   IN: " + interval);
        if (parsed > interval_in_seconds) {
          System.out.println("final interval: " + interval + "  Parsed: " + parsed);
          interval_in_seconds = parsed;
          string_interval = interval;
        }
      }
    }
    
    if (skip_cache) {
      // don't bother doing anything else.
      return Deferred.fromResult(null);
    }
    
    // TODO - in the future use rollup config. For now snap to one day.
    // AND 
    if (interval_in_seconds >= 3600) {
      interval_in_seconds = 86400;
      string_interval = "1d";
    } else {
      interval_in_seconds = 3600;
      string_interval = "1h";
    }
    
    if (ds_interval == Integer.MAX_VALUE) {
      min_interval = 0;
    } else {
      min_interval = ds_interval;
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
    
    keys = key_gen.generate(context.query().buildHashCode().asLong(), string_interval, slices);
    if (tip_query) {
      keys = Arrays.copyOf(keys, keys.length - 1);
    }
    results = new ResultOrSubQuery[slices.length];
    
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
    hits = new AtomicInteger();
    latch = new AtomicInteger(slices.length);
    cache.fetch(this, keys, this, null);
  }
  
  @Override
  public void onCacheResult(final CacheQueryResult result) {
    if (failed.get()) {
      return;
    }
    
    try {
      ResultOrSubQuery ros = null;
      int idx = 0;
      for (int i = 0; i < keys.length; i++) {
        if (Bytes.memcmp(keys[i], result.key()) == 0) {
          synchronized (results) {
            results[i] = new ResultOrSubQuery();
            results[i].key = result.key();
            results[i].map = result.results();
          }
          ros = results[i];
          idx = i;
          break;
        }
      }
      
      if (ros == null) {
        onCacheError(new RuntimeException("Whoops, got a result that wasn't in "
            + "our keys? " + Arrays.toString(result.key())));
        return;
      }
      
      if (ros.map == null || ros.map.isEmpty()) {
        System.out.println("  UH OH No hit!");
        // TODO - configure the threshold
        if (okToRunMisses(hits.get())) {        
          ros.sub_context = ros.sub_context = buildQuery(slices[idx], slices[idx] + interval_in_seconds, context, ros);
          ros.sub_context.initialize(null)
            .addCallback(new SubQueryCB(ros.sub_context))
            .addErrback(new ErrorCB());
        }
      } else {
        if (okToRunMisses(hits.incrementAndGet())) {
          runCacheMissesAfterSatisfyingPercent();
        }
        if (requestTip(idx, result.lastValueTimestamp())) {
          System.out.println("     TIP QUERY but not at final latch");
          ros.map = null;
          if (okToRunMisses(hits.get())) {
            latch.incrementAndGet();
            ros.sub_context = ros.sub_context = buildQuery(slices[idx], slices[idx] + interval_in_seconds, context, ros);
            ros.sub_context.initialize(null)
              .addCallback(new SubQueryCB(ros.sub_context))
              .addErrback(new ErrorCB());
          }
        } else {
          ros.complete.set(true);
        }
      }
      
      if (latch.decrementAndGet() == 0) {
        // all results are in! Now see if we're a tip and if so then we need to see
        // if we need to run
        if (requestTip(idx, result.lastValueTimestamp())) {
          ros.map = null;
          System.out.println("     TIP QUERY at final latch");
          if (okToRunMisses(hits.get())) {
            latch.incrementAndGet();
            ros.sub_context = buildQuery(slices[idx], slices[idx] + interval_in_seconds, context, ros);
            ros.sub_context.initialize(null)
              .addCallback(new SubQueryCB(ros.sub_context))
              .addErrback(new ErrorCB());
          }
          return;
        }
        
        // all cache are in, see if we should send up or if we need to fire
        // sub queries.
        run();
      }
    } catch (Throwable t) {
      onCacheError(t);
    }
  }

  @Override
  public void onCacheError(final Throwable t) {
    if (failed.compareAndSet(false, true)) {
      LOG.warn("Failure from cache", t);
      onError(t);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Failure from cache after initial failure", t);
    }
  }
  
  // MERGE AND SEND!
  public void run() {
    if (hits.get() < keys.length) {
      for (int i = 0; i < results.length; i++) {
        if (!results[i].complete.get()) {
          System.out.println("        not done??: " + i);
          if (okToRunMisses(hits.get())) {
            runCacheMissesAfterSatisfyingPercent();
          } else {
            // We failed the cache threshold so we run a FULL query.
            System.out.println(" RUN THE FULL QUERY!!!!!!!");
            sub_context = buildQuery(
                slices[0], 
                slices[slices.length - 1] + interval_in_seconds, 
                context, 
                new FullQuerySink());
            sub_context.initialize(null)
                .addCallback(new SubQueryCB(sub_context))
                .addErrback(new ErrorCB());
          }
          return;
        }
      }
    }
    
    try {
      // sort and merge
      Map<String, QueryResult[]> sorted = Maps.newHashMap();
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          System.out.println("null at................... " + i);
          continue;
        }
        for (final Entry<String, QueryResult> entry : results[i].map.entrySet()) {
          QueryResult[] qrs = sorted.get(entry.getKey());
          if (qrs == null) {
            qrs = new QueryResult[results.length + (tip_query ? 1 : 0)];
            sorted.put(entry.getKey(), qrs);
          }
          qrs[i] = entry.getValue();
        }
      }
      
      latch.set(sorted.size());
      for (final QueryResult[] results : sorted.values()) {
        // TODO - implement
        // TODO - send in thread pool
        for (final QuerySink sink : sinks) {
          sink.onNext(new CombinedResult(results));
        }
      }
      
      for (int i = 0; i < results.length; i++) {
        final int x = i;
        if (results[i].sub_context == null) {
          continue;
        }
        
        // write to the cache
        // TODO - mode
        context.tsdb().getQueryThreadPool().submit(new Runnable() {
          @Override
          public void run() {
            cache.cache(0, keys[x], results[x].map.values());
          }
        }, context);
      }
    
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  /** @return if we found a query we couldn't cache. */
  public boolean skipCache() {
    return skip_cache;
  }
  
  class ResultOrSubQuery implements QuerySink {
    byte[] key;
    QueryContext sub_context;
    volatile Map<String, QueryResult> map = Maps.newConcurrentMap();
    AtomicBoolean complete = new AtomicBoolean();
    
    @Override
    public void onComplete() {
      if (failed.get()) {
        return;
      }
      
      complete.compareAndSet(false, true);
      if (latch.decrementAndGet() == 0) {
        System.out.println("[[[[[[[[ COMPLETE!!! ]]]]]]] RUNNING");
        run();
      }
    }
    
    @Override
    public void onNext(final QueryResult next) {
      if (failed.get()) {
        return;
      }
      
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
      System.out.println(" SET IT!!!!!!!!!");
    }
    
    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
    }
    
    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        ReadCacheQueryPipelineContext.this.onError(t);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in sub query after initial failure", t);
        }
      }
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

  static QueryContext buildQuery(final int start, 
                                 final int end, 
                                 final QueryContext context, 
                                 final QuerySink sink) {
    final SemanticQuery.Builder builder = ((SemanticQuery) context.query())
        .toBuilder();
    builder.setStart(Integer.toString(start));
    builder.setEnd(Integer.toString(end));
    
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
  
  void runCacheMissesAfterSatisfyingPercent() {
    System.out.println(" --------- RUNNING OTHERS");
    synchronized (results) {
      for (int i = 0; i < results.length; i++) {
        final ResultOrSubQuery ros = results[i];
        if (ros == null) {
          continue;
        }
        
        if (ros.sub_context == null && 
            (ros.map == null || ros.map.isEmpty())) {
          System.out.println("        STARTing: " + i);
          latch.incrementAndGet();
          ros.sub_context = buildQuery(slices[i], slices[i] + 
              interval_in_seconds, context, ros);
          ros.sub_context.initialize(null)
            .addCallback(new SubQueryCB(ros.sub_context))
            .addErrback(new ErrorCB());
        }
      }
    }
  }
  
  boolean okToRunMisses(final int hits) {
    System.out.println("       HITS %: " + ((double) hits) / (double) keys.length);
    return hits > 0 && ((double) hits / (double) keys.length) > .60;
  }
  
  boolean requestTip(final int index, final TimeStamp ts) {
    // if the index is earlier than the final two buckets then we know we
    // don't need to request any data as it's old enough.
    if (index < results.length - 3) {
      System.out.println(" IDX : " + index);
      return false;
    }
    
    System.out.println("DELTA: " + (current_time - ts.msEpoch()) + "  MIN: " + (min_interval * 1000));
    if (current_time - ts.msEpoch() > (min_interval * 1000)) {
      return false;
    }
    return true;
  }
  
  class SubQueryCB implements Callback<Void, Void> {
    final QueryContext context;
    
    SubQueryCB(final QueryContext context) {
      this.context = context;
    }
    
    @Override
    public Void call(final Void arg) throws Exception {
      context.fetchNext(null);
      return null;
    }
    
  }
  
  class ErrorCB implements Callback<Void, Exception> {

    @Override
    public Void call(final Exception e) throws Exception {
      if (failed.compareAndSet(false, true)) {
        onError(e);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in sub query after initial failure", e);
        }
      }
      return null;
    }
    
  }

  class FullQuerySink implements QuerySink {

    @Override
    public void onComplete() {
      if (latch.decrementAndGet() == 0) {
        for (final QuerySink sink : sinks) {
          sink.onComplete();
        }
      }
      
      // write to the cache
      // TODO - mode
      context.tsdb().getQueryThreadPool().submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.cache(slices, keys, sub_results);
          } catch (Throwable t) {
            LOG.error("Failed to cache the data", t);
          } finally {
            for (int i = 0; i < sub_results.size(); i++) {
              sub_results.get(i).close();
            }
          }
        }
      }, context);
    }
    
    @Override
    public void onNext(final QueryResult next) {
      latch.incrementAndGet();
      synchronized (ReadCacheQueryPipelineContext.this) {
        if (sub_results == null) {
          sub_results = Lists.newArrayList();
        }
        sub_results.add(next);
      }
      ReadCacheQueryPipelineContext.this.onNext(next);
    }
    
    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
    }
    
    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        ReadCacheQueryPipelineContext.this.onError(t);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in main query after initial failure", t);
        }
      }
    }
    
  }
}
