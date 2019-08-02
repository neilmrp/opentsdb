package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;

public class TestReadCacheQueryPipelineContext {

  private static MockTSDB TSDB;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static TimeSeriesDataSourceFactory STORE_FACTORY;
  private static List<TimeSeriesDataSource> STORE_NODES;
  
  private TimeSeriesQuery query;
  private QueryContext context;
  private QuerySink sink;
  private QuerySinkConfig sink_config;
  private QueryCachePlugin cache_plugin;
  private TimeSeriesCacheKeyGenerator keygen_plugin;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    STORE_FACTORY = mock(TimeSeriesDataSourceFactory.class);
    STORE_NODES = Lists.newArrayList();
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    
    DownsampleFactory factory = new DownsampleFactory();
    factory.initialize(TSDB, null).join();
    when(TSDB.registry.getDefaultPlugin(DownsampleFactory.class)).thenReturn(factory);
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    sink = mock(QuerySink.class);
    sink_config = mock(QuerySinkConfig.class);
    cache_plugin = mock(QueryCachePlugin.class);
    keygen_plugin = mock(TimeSeriesCacheKeyGenerator.class);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514786400")
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build())
        .build();
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(TSDB.getRegistry().getPlugin(eq(QueryCachePlugin.class), anyString()))
      .thenReturn(cache_plugin);
    when(TSDB.getRegistry().getPlugin(eq(TimeSeriesCacheKeyGenerator.class), anyString()))
      .thenReturn(keygen_plugin);
  }
  
  @Test
  public void ctor() throws Exception {
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
  }
  
  @Test
  public void initializeNoDownsample() throws Exception {
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(3600, ctx.interval_in_seconds);
    assertEquals(6, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 6; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  @Test
  public void initializeNoDownsampleOffsetQueryTimes() throws Exception {
    setQuery(1514768087, 1514789687, null, false);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(3600, ctx.interval_in_seconds);
    assertEquals(7, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 7; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  @Test
  public void initializeSingleDownsample1m() throws Exception {
    setQuery(1514764800, 1514786400, "1m", false);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(3600, ctx.interval_in_seconds);
    assertEquals(6, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 6; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  @Test
  public void initializeSingleDownsample1h() throws Exception {
    setQuery(1514764800, 1514786400, "1h", false);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(86400, ctx.interval_in_seconds);
    assertEquals(1, ctx.slices.length);
    assertEquals(1514764800, ctx.slices[0]);
  }
  
  @Test
  public void initializeSingleDownsampleRunAllSmall() throws Exception {
    setQuery(1514764800, 1514786400, "1m", true);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(3600, ctx.interval_in_seconds);
    assertEquals(6, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 6; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  @Test
  public void initializeSingleDownsampleRunAllBig() throws Exception {
    setQuery(1514764800, 1514876087, "1m", true);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(86400, ctx.interval_in_seconds);
    assertEquals(2, ctx.slices.length);
    assertEquals(1514764800, ctx.slices[0]);
    assertEquals(1514851200, ctx.slices[1]);
  }
  
  @Test
  public void initializeSingleDownsampleAutoSmall() throws Exception {
    setQuery(1514764800, 1514786400, "auto", false);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(3600, ctx.interval_in_seconds);
    assertEquals(6, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 6; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  @Test
  public void initializeSingleDownsampleAutoBig() throws Exception {
    setQuery(1514764800, 1515048887, "auto", false);
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
    
    assertSame(cache_plugin, ctx.cache);
    assertSame(keygen_plugin, ctx.key_gen);
    assertEquals(86400, ctx.interval_in_seconds);
    assertEquals(4, ctx.slices.length);
    int ts = 1514764800;
    for (int i = 0; i < 4; i++) {
      assertEquals(ts, ctx.slices[i]);
      ts += ctx.interval_in_seconds;
    }
  }
  
  void setQuery(final int start, 
                final int end, 
                final String interval, 
                final boolean runall) {
    SemanticQuery.Builder builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(start))
        .setEnd(Integer.toString(end))
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setId("m1")
            .build());
    if (!Strings.isNullOrEmpty(interval)) {
      builder.addExecutionGraphNode(DownsampleConfig.newBuilder()
          .setAggregator("sum")
          .setInterval(interval)
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .setRunAll(runall)
          .addSource("m1")
          .setId("downsample")
          .build());
    }
    query = builder.build();
    when(context.query()).thenReturn(query);
  }
}
