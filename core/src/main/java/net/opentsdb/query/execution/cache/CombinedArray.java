package net.opentsdb.query.execution.cache;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

public class CombinedArray implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, NumericArrayType {
  
  int idx = 0;
  boolean called = false;
  long[] long_array;
  double[] double_array;
  TimeStamp timestamp;
  
  CombinedArray(final CombinedResult result, final List<Pair<QueryResult, TimeSeries>> series) {
    for (int i = 0; i < series.size(); i++) {
      final TypedTimeSeriesIterator<NumericArrayType> iterator = 
          (TypedTimeSeriesIterator<NumericArrayType>) 
            series.get(i).getValue().iterator(NumericArrayType.TYPE).get();
      final TimeSeriesValue<NumericArrayType> value = iterator.next();
      final int array_length = (int) ((result.timeSpecification().end().epoch() - 
          result.timeSpecification().start().epoch()) /
          result.timeSpecification().interval().get(ChronoUnit.SECONDS));
      System.out.println("          ARRAY LEN: " + array_length);
      if (i == 0) {
        timestamp = value.timestamp().getCopy();
        // get the value offset since we may have data before the query start time.
        int delta = 0;
        if (result.timeSpecification().start().compare(Op.GT, series.get(i).getKey().timeSpecification().start())) {
          delta = (int) ((result.timeSpecification().start().epoch() - series.get(i).getKey().timeSpecification().start().epoch()) / result.timeSpecification().interval().get(ChronoUnit.SECONDS));
        } else if (series.get(i).getKey().timeSpecification().start().compare(Op.GT, result.timeSpecification().start())) {
          // fill
          TimeStamp ts = result.timeSpecification().start().getCopy();
          while (series.get(i).getKey().timeSpecification().start().compare(Op.GT, ts)) {
            ts.add(result.timeSpecification().interval());
            idx++;
          }
        }
        System.out.println("        DELTA: " + delta);
        if (value.value().isInteger()) {
          if (idx > 0) {
            // fills!
            double_array = new double[array_length];
            Arrays.fill(double_array, Double.NaN);
            for (int x = value.value().offset() + delta; x < value.value().end(); x++) {
              double_array[idx++] = value.value().longArray()[x];
            }
          } else {
            long_array = new long[array_length];
            System.arraycopy(value.value().longArray(), value.value().offset() + delta, long_array, idx, value.value().end());
          }
        } else {
          double_array = new double[array_length];
          Arrays.fill(double_array, Double.NaN);
          System.arraycopy(value.value().doubleArray(), value.value().offset() + delta, double_array, idx, value.value().end());
        }
        idx = value.value().end();
      } else {
        // size is important
        int end = 0;
        if (i == series.size() - 1) {
          // tail
          System.out.println("               TAIL!");
          int delta = 0;
          if (result.timeSpecification().end().compare(Op.LT, series.get(i).getKey().timeSpecification().end())) {
            delta = (int) ((series.get(i).getKey().timeSpecification().end().epoch() - result.timeSpecification().end().epoch()) / result.timeSpecification().interval().get(ChronoUnit.SECONDS));
          }
          end = value.value().end() - delta;
        } else {
          end = value.value().end();
        }
        int expected = end - value.value().offset() + idx;
        if (long_array != null && expected > long_array.length) {
          // TODO - grow!!
          throw new IllegalStateException("Whoops!  Array was " + long_array.length + "  but needed " + expected);
        } else if (double_array != null && expected > double_array.length) {
          // TODO - grow!!
          throw new IllegalStateException("Whoops!");
        }
        
        if (value.value().isInteger()) {
          if (long_array == null) {
            for (int x = value.value().offset(); x < end; x++) {
              double_array[idx++] = value.value().longArray()[x];
            }
            //System.arraycopy(value.value().doubleArray(), value.value().offset(), double_array, idx, value.value().end());
          } else {
            System.arraycopy(value.value().longArray(), value.value().offset(), long_array, idx, end);
            idx += value.value().end();
          }
        } else {
          if (double_array == null) {
            double_array = new double[long_array.length];
            Arrays.fill(double_array, Double.NaN);
            for (int x = 0; x < idx; x++) {
              double_array[x] = long_array[x];
            }
            long_array = null;
          }
          System.arraycopy(value.value().doubleArray(), value.value().offset(), double_array, idx, value.value().end());
          idx += value.value().end();
        }
      }
      series.get(i).getValue().close();
    }
    System.out.println("DONE.........: " + idx);
  }

  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    called = true;
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
    return idx;
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
