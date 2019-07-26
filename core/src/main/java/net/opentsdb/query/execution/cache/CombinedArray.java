package net.opentsdb.query.execution.cache;

import java.util.Arrays;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;

public class CombinedArray implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, NumericArrayType {
  
  int idx = 0;
  boolean called = false;
  long[] long_array;
  double[] double_array;
  TimeStamp timestamp;
  
  CombinedArray(final List<TimeSeries> series) {
    for (int i = 0; i < series.size(); i++) {
      final TypedTimeSeriesIterator<NumericArrayType> iterator = 
          (TypedTimeSeriesIterator<NumericArrayType>) 
            series.get(idx++).iterator(NumericType.TYPE).get();
      final TimeSeriesValue<NumericArrayType> value = iterator.next();
      if (i == 0) {
        timestamp = value.timestamp().getCopy();
        // setup the initial size as everything should be the same
        int size = value.value().end() - value.value().offset();
        size *= series.size();
        if (value.value().isInteger()) {
          long_array = new long[size];
          System.arraycopy(value.value().longArray(), value.value().offset(), long_array, idx, value.value().end());
        } else {
          double_array = new double[size];
          System.arraycopy(value.value().doubleArray(), value.value().offset(), double_array, idx, value.value().end());
        }
        idx = value.value().end();
      } else {
        // size is important
        int expected = value.value().end() - value.value().offset() + idx;
        if (long_array != null && expected >= long_array.length) {
          // TODO - grow!!
          throw new IllegalStateException("Whoops!");
        } else if (expected >= double_array.length) {
          // TODO - grow!!
          throw new IllegalStateException("Whoops!");
        }
        
        if (value.value().isInteger()) {
          if (long_array == null) {
            System.arraycopy(value.value().longArray(), value.value().offset(), double_array, idx, value.value().end());
          } else {
            System.arraycopy(value.value().longArray(), value.value().offset(), long_array, idx, value.value().end());
          }
        } else {
          System.arraycopy(value.value().doubleArray(), value.value().offset(), double_array, idx, value.value().end());
        }
        idx += value.value().end();
      }
      series.get(i).close();
    }
  }

  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return long_array != null ? long_array.length : double_array.length;
  }

  @Override
  public boolean isInteger() {
    return long_array != null;
  }

  @Override
  public long[] longArray() {
    return long_array;
  }

  @Override
  public double[] doubleArray() {
    return double_array;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericArrayType value() {
    return this;
  }
}
