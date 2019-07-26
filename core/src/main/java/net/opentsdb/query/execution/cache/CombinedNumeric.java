package net.opentsdb.query.execution.cache;

import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;

public class CombinedNumeric implements TypedTimeSeriesIterator<NumericType> {
  List<TimeSeries> series;
  int idx = 0;
  TypedTimeSeriesIterator<NumericType> iterator;
  
  CombinedNumeric(final List<TimeSeries> series) {
    System.out.println(" COMBINED NUMERIC WITH: " + series.size());
    this.series = series;
    iterator = (TypedTimeSeriesIterator<NumericType>) 
        series.get(idx++).iterator(NumericType.TYPE).get();
  }

  @Override
  public boolean hasNext() {
    if (idx >= series.size()) {
      return false;
    }
    // TODO - may need to skip some
    return iterator.hasNext();
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    TimeSeriesValue<NumericType> value = iterator.next();
    if (!iterator.hasNext()) {
      if (idx < series.size()) {
        // TODO - may need to skip some
        iterator = (TypedTimeSeriesIterator<NumericType>) 
            series.get(idx++).iterator(NumericType.TYPE).get();
      } else {
        iterator = null;
      }
    }
    return value;
  }

  @Override
  public TypeToken<NumericType> getType() {
    return NumericType.TYPE;
  }
}
