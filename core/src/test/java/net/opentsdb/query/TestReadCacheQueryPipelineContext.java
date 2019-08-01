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

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;

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
        .setEnd("1514768400")
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
  public void initialize() throws Exception {
    ReadCacheQueryPipelineContext ctx = new ReadCacheQueryPipelineContext(context,
        Lists.newArrayList());
    ctx.initialize(null).join();
  }
}
