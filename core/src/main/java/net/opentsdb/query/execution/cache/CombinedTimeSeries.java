package net.opentsdb.query.execution.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

public class CombinedTimeSeries implements TimeSeries {
  final CombinedResult combined;
  List<Pair<QueryResult, TimeSeries>> series;
  
  CombinedTimeSeries(final CombinedResult combined, final QueryResult result, final TimeSeries ts) {
    this.combined = combined;
    System.out.println("NEW TIMESERIES: " + ts.id());
    series = Lists.newArrayList();
    series.add(new Pair<>(result, ts));
  }
  
  @Override
  public TimeSeriesId id() {
    return series.get(0).getValue().id();
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    System.out.println(" !!!!!!!!!! ASKING FOR: " + type);
    if (series.get(0).getValue().types().contains(type)) {
      if (type == NumericType.TYPE) {
        return Optional.of(new CombinedNumeric(combined, series));
      } else if (type == NumericArrayType.TYPE) {
        return Optional.of(new CombinedArray(combined, series));
      } else if (type == NumericSummaryType.TYPE) {
        //return Optional.of(new CombinedSummary(series));
      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
        Lists.newArrayList();
    TypeToken<? extends TimeSeriesDataType> type = series.get(0).getValue().types().iterator().next();
    if (type == NumericType.TYPE) {
      iterators.add(new CombinedNumeric(combined, series));
    } else if (type == NumericArrayType.TYPE) {
      iterators.add(new CombinedArray(combined, series));
    } else if (type == NumericSummaryType.TYPE) {
      //iterators.add(new CombinedSummary(series));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return series.get(0).getValue().types();
  }

  @Override
  public void close() {
//    for (final TimeSeries ts : series) {
//      ts.close();
//    }
  }

}
