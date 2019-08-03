package net.opentsdb.query.execution.cache;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.rollup.RollupConfig;

public class CombinedResult implements QueryResult, TimeSpecification {

  Map<Long, TimeSeries> time_series;
  TimeStamp spec_start;
  TimeSpecification spec;
  QueryNode node;
  String data_source;
  final List<QuerySink> sinks;
  final AtomicInteger latch;
  
  public CombinedResult(final QueryResult[] results, final List<QuerySink> sinks, final AtomicInteger latch) {
    this.sinks = sinks;
    this.latch = latch;
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
//    for (final TimeSeries ts : time_series.values()) {
//      ts.close();
//    }
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
