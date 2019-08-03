package net.opentsdb.query.execution.cache;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TypedTimeSeriesIterator;

public class CombinedTimeSeries implements TimeSeries {
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
//      if (type == NumericType.TYPE) {
//        return Optional.of(new CombinedNumeric(series));
//      } else if (type == NumericArrayType.TYPE) {
//        return Optional.of(new CombinedArray(series));
//      } else if (type == NumericSummaryType.TYPE) {
//        return Optional.of(new CombinedSummary(series));
//      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
        Lists.newArrayList();
    TypeToken<? extends TimeSeriesDataType> type = series.get(0).types().iterator().next();
//    if (type == NumericType.TYPE) {
//      iterators.add(new CombinedNumeric(series));
//    } else if (type == NumericArrayType.TYPE) {
//      iterators.add(new CombinedArray(series));
//    } else if (type == NumericSummaryType.TYPE) {
//      iterators.add(new CombinedSummary(series));
//    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return series.get(0).types();
  }

  @Override
  public void close() {
//    for (final TimeSeries ts : series) {
//      ts.close();
//    }
  }

}
