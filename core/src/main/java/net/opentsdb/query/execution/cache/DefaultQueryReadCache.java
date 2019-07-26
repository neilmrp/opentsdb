//package net.opentsdb.query.execution.cache;
//
//import java.time.ZoneId;
//import java.time.temporal.ChronoUnit;
//import java.time.temporal.TemporalAmount;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Optional;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.common.base.Strings;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.reflect.TypeToken;
//import com.stumbleupon.async.Deferred;
//
//import net.opentsdb.common.Const;
//import net.opentsdb.core.TSDB;
//import net.opentsdb.data.PartialTimeSeries;
//import net.opentsdb.data.TimeSeries;
//import net.opentsdb.data.TimeSeriesDataSource;
//import net.opentsdb.data.TimeSeriesDataType;
//import net.opentsdb.data.TimeSeriesId;
//import net.opentsdb.data.TimeSpecification;
//import net.opentsdb.data.TimeStamp;
//import net.opentsdb.data.TypedTimeSeriesIterator;
//import net.opentsdb.data.types.numeric.NumericArrayType;
//import net.opentsdb.data.types.numeric.NumericSummaryType;
//import net.opentsdb.data.types.numeric.NumericType;
//import net.opentsdb.query.QueryContext;
//import net.opentsdb.query.QueryNode;
//import net.opentsdb.query.QueryNodeConfig;
//import net.opentsdb.query.QueryPipelineContext;
//import net.opentsdb.query.QueryResult;
//import net.opentsdb.query.QuerySink;
//import net.opentsdb.query.QuerySinkCallback;
//import net.opentsdb.query.TimeSeriesDataSourceConfig;
//import net.opentsdb.query.TimeSeriesQuery;
//import net.opentsdb.query.execution.cache.QueryCachePlugin.CacheCB;
//import net.opentsdb.query.execution.cache.QueryCachePlugin.CacheQueryResult;
//import net.opentsdb.query.processor.downsample.DownsampleConfig;
//import net.opentsdb.query.processor.downsample.DownsampleFactory;
//import net.opentsdb.rollup.RollupConfig;
//import net.opentsdb.stats.Span;
//import net.opentsdb.utils.Bytes;
//import net.opentsdb.utils.Bytes.ByteMap;
//import net.opentsdb.utils.DateTime;
//
//public class DefaultQueryReadCache implements TimeSeriesDataSource, CacheCB {
//  private static final Logger LOG = LoggerFactory.getLogger(DefaultQueryReadCache.class);
//  
//  int[] slices;
//  
//  QueryPipelineContext context;
//  
//  TimeSeriesQuery original_query;
//  
//  TimeSeriesDataSourceConfig config;
//  
//  QueryCachePlugin cache;
//  
//  TimeSeriesCacheKeyGenerator key_gen;
//  
//  boolean tip_query;
//  
//  TSDB tsdb;
//  
//  byte[][] keys;
//
//  MissOrTip[] fills;
//  //MissOrTip tip;
//  
////  ByteMap<List<QueryResult>> results;
////  ByteMap<MissOrTip> fills;
//  AtomicInteger latch;
//  
//  @Override
//  public QueryPipelineContext pipelineContext() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public Deferred initialize(Span span) {
//    // slice n dice
//    
//    // TODO properly find a downsample
//    String interval = null;
//    for (final QueryNodeConfig config : original_query.getExecutionGraph()) {
//      if (config instanceof DownsampleConfig) {
//        if (((DownsampleConfig) config).getInterval().toLowerCase().equals("auto")) {
//          long delta = context.query().endTime().msEpoch() - context.query().startTime().msEpoch();
//          final DownsampleFactory factory = tsdb.getRegistry().getDefaultPlugin(DownsampleFactory.class);
//          interval = DownsampleFactory.getAutoInterval(delta, factory.intervals());
//        } else {
//          interval = ((DownsampleConfig) config).getInterval();
//        }
//        
//        // TODO - we need to walk the graph
//        break;
//      }
//    }
//    if (Strings.isNullOrEmpty(interval)) {
//      interval = "1h";
//    }
//    
//    // TODO - pull from rollup configs
//    // TODO - validate calendaring. May need to snap differently based on timezone.
//    int mod = DateTime.parseDuration(interval) >= 3600000 ? 86400 : 3600; 
//    long start = context.query().startTime().epoch();
//    start = start - (start % mod);
//    
//    long end = context.query().endTime().epoch();
//    end = end - (end % mod);
//    if (end != context.query().endTime().epoch()) {
//      end += mod;
//    }
//    
//    slices = new int[(int) ((end - start) / mod)];
//    int ts = (int) start;
//    for (int i = 0; i < slices.length; i++) {
//      slices[i] = ts;
//      ts += mod;
//    }
//    
//    keys = key_gen.generate(original_query.buildHashCode().asLong(), slices);
//    if (tip_query) {
//      keys = Arrays.copyOf(keys, keys.length - 1);
//    }
//    fills = new MissOrTip[slices.length];
//    return null;
//  }
//
//  @Override
//  public QueryNodeConfig config() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public void close() {
//    // TODO Auto-generated method stub
//    
//  }
//
//  @Override
//  public void onComplete(QueryNode downstream, long final_sequence,
//      long total_sequences) {
//    // TODO Auto-generated method stub
//    
//  }
//
//  @Override
//  public void onNext(final QueryResult next) {
//    
//  }
//
//  @Override
//  public void onNext(PartialTimeSeries next) {
//    // TODO Auto-generated method stub
//    
//  }
//
//  @Override
//  public void onError(Throwable t) {
//    // TODO Auto-generated method stub
//    
//  }
//  
//  @Override
//  public void fetchNext(final Span span) {
//    // TODO - timeless
//    if (tip_query) {
//      latch = new AtomicInteger(keys.length);
//    } else {
//      latch = new AtomicInteger(keys.length);
//    }
//    
//    cache.fetch(context, keys, this, null);
//    
//    if (tip_query) {
//      fills[fills.length - 1] = new MissOrTip();
//      fills[fills.length - 1].key = keys[keys.length - 1];
//      QueryContext ctx = null;
//      // TODO - build query
//      
//      // TODO - fire off the query;
//    }
//  }
//
//  @Override
//  public String[] setIntervals() {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  @Override
//  public void onCacheResult(final CacheQueryResult result) {
//    MissOrTip mot = null;
//    for (int i = 0; i < keys.length; i++) {
//      if (Bytes.memcmp(keys[i], result.key()) == 0) {
//        fills[i] = new MissOrTip();
//        mot.key = result.key();
//        fills[i].results = result.results();
//        break;
//      }
//    }
//    
//    if (result.results() == null || result.results().isEmpty()) {
//      QueryContext ctx = null;
//      // TODO - build query
//      mot.sub_context = ctx;
//      
//      // TODO - fire off the query;
//    } else if (latch.decrementAndGet() == 0) {
//      run();
//    }
//  }
//
//  @Override
//  public void onCacheError(Throwable t) {
//    // TODO Auto-generated method stub
//    
//  }
//
//  // MERGE AND SEND!
//  public void run() {
//    // sort and merge
//    Map<String, QueryResult[]> sorted = Maps.newHashMap();
//    for (int i = 0; i < fills.length; i++) {
//      for (final Entry<String, QueryResult> entry : fills[i].results.entrySet()) {
//        QueryResult[] qrs = sorted.get(entry.getKey());
//        if (qrs == null) {
//          qrs = new QueryResult[fills.length + (tip_query ? 1 : 0)];
//          sorted.put(entry.getKey(), qrs);
//        }
//        qrs[i] = entry.getValue();
//      }
//    }
//    
//    // TODO tip
//    
//    for (final QueryResult[] results : sorted.values()) {
//      // TODO - implement
//      // TODO - send in thread pool
//      sendUpstream(new CombinedResult(results));
//    }
//  }
//  
//  class MissOrTip implements QuerySink {
//    byte[] key;
//    QueryContext sub_context;
//    Map<String, QueryResult> results;
//    AtomicBoolean complete = new AtomicBoolean();
//    
//    @Override
//    public void onComplete() {
//      complete.compareAndSet(false, true);
//      if (latch.decrementAndGet() == 0) {
//        run();
//      }
//    }
//    
//    @Override
//    public synchronized void onNext(QueryResult next) {
//      if (results == null) {
//        results = Maps.newHashMap();
//      }
//      results.put(next.source().config().getId() + ":" + next.dataSource(), next);
//    }
//    
//    @Override
//    public void onNext(PartialTimeSeries next, QuerySinkCallback callback) {
//      // TODO Auto-generated method stub
//      
//    }
//    
//    @Override
//    public void onError(Throwable t) {
//      // TODO Auto-generated method stub
//      
//    }
//  }
//  
//  class CombinedTimeSeries implements TimeSeries {
//    List<TimeSeries> series;
//    
//    CombinedTimeSeries(final TimeSeries ts) {
//      series = Lists.newArrayList();
//      series.add(ts);
//    }
//    
//    @Override
//    public TimeSeriesId id() {
//      return series.get(0).id();
//    }
//
//    @Override
//    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
//        TypeToken<? extends TimeSeriesDataType> type) {
//      if (series.get(0).types().contains(type)) {
//        if (type == NumericType.TYPE) {
//          return Optional.of(new CombinedNumeric(series));
//        } else if (type == NumericArrayType.TYPE) {
//          return Optional.of(new CombinedArray(series));
//        } else if (type == NumericSummaryType.TYPE) {
//          return Optional.of(new CombinedSummary(series));
//        }
//      }
//      return Optional.empty();
//    }
//
//    @Override
//    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
//      List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
//          Lists.newArrayList();
//      TypeToken<? extends TimeSeriesDataType> type = series.get(0).types().iterator().next();
//      if (type == NumericType.TYPE) {
//        iterators.add(new CombinedNumeric(series));
//      } else if (type == NumericArrayType.TYPE) {
//        iterators.add(new CombinedArray(series));
//      } else if (type == NumericSummaryType.TYPE) {
//        iterators.add(new CombinedSummary(series));
//      }
//      return iterators;
//    }
//
//    @Override
//    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
//      return series.get(0).types();
//    }
//
//    @Override
//    public void close() {
//      for (final TimeSeries ts : series) {
//        ts.close();
//      }
//    }
//    
//  }
//  
//  class CombinedResult implements QueryResult, TimeSpecification {
//
//    Map<Long, TimeSeries> time_series;
//    TimeStamp spec_start;
//    TimeSpecification spec;
//    
//    CombinedResult(final QueryResult[] results) {
//      time_series = Maps.newHashMap();
//      for (int i = 0; i < results.length; i++) {
//        if (results[i] == null) {
//          continue;
//        }
//        
//        if (spec_start == null && results[i].timeSpecification() != null) {
//          spec_start = results[i].timeSpecification().start();
//        }
//        
//        // TODO more time spec
//        spec = results[i].timeSpecification();
//        
//        // TODO handle tip merge eventually
//        for (final TimeSeries ts : results[i].timeSeries()) {
//          final long hash = ts.id().buildHashCode();
//          TimeSeries combined = time_series.get(hash);
//          if (combined == null) {
//            combined = new CombinedTimeSeries(ts);
//            time_series.put(hash, combined);
//          } else {
//            ((CombinedTimeSeries) combined).series.add(ts);
//          }
//        }
//      }
//    }
//    
//    @Override
//    public TimeSpecification timeSpecification() {
//      return spec_start == null ? null : this;
//    }
//
//    @Override
//    public Collection<TimeSeries> timeSeries() {
//      return time_series.values();
//    }
//
//    @Override
//    public String error() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public Throwable exception() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public long sequenceId() {
//      // TODO Auto-generated method stub
//      return 0;
//    }
//
//    @Override
//    public QueryNode source() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public String dataSource() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public TypeToken<? extends TimeSeriesId> idType() {
//      return Const.TS_STRING_ID;
//    }
//
//    @Override
//    public ChronoUnit resolution() {
//      return ChronoUnit.SECONDS;
//    }
//
//    @Override
//    public RollupConfig rollupConfig() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public void close() {
//      for (final TimeSeries ts : time_series.values()) {
//        ts.close();
//      }
//    }
//
//    @Override
//    public TimeStamp start() {
//      return spec_start;
//    }
//
//    @Override
//    public TimeStamp end() {
//      return spec.end();
//    }
//
//    @Override
//    public TemporalAmount interval() {
//      return spec.interval();
//    }
//
//    @Override
//    public String stringInterval() {
//      return spec.stringInterval();
//    }
//
//    @Override
//    public ChronoUnit units() {
//      return spec.units();
//    }
//
//    @Override
//    public ZoneId timezone() {
//      return spec.timezone();
//    }
//
//    @Override
//    public void updateTimestamp(int offset, TimeStamp timestamp) {
//      spec.updateTimestamp(offset, timestamp);
//    }
//
//    @Override
//    public void nextTimestamp(TimeStamp timestamp) {
//      spec.nextTimestamp(timestamp);
//    }
//    
//  }
//}
