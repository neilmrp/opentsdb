package net.opentsdb.query.execution.cache;

import java.util.List;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;

public class CombinedSummary implements TypedTimeSeriesIterator<NumericSummaryType> {

  MutableNumericSummaryValue value;
  boolean called = false;
  
  CombinedSummary(final List<TimeSeries> series) {
    value = new MutableNumericSummaryValue();
    
    for (int i = 0; i < series.size(); i++) {
      Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = series.get(i).iterator(NumericSummaryType.TYPE);
      if (!op.isPresent()) {
        continue;
      }
      TypedTimeSeriesIterator<NumericSummaryType> it = (TypedTimeSeriesIterator<NumericSummaryType>) op.get();
      TimeSeriesValue<NumericSummaryType> val = it.next();
      if (i == 0) {
        value.reset(val);
      } else {
        for (final int summary : val.value().summariesAvailable()) {
          NumericType new_dp = val.value().value(summary);
          NumericType existing = value.value(summary);
          switch (summary) {
          case 0:
          case 1:
            if (new_dp.isInteger() && existing.isInteger()) {
              value.resetValue(summary, new_dp.longValue() + existing.longValue());
            } else {              
              value.resetValue(summary, new_dp.toDouble() + existing.toDouble());
            }
            break;
          case 2:
            if (new_dp.toDouble() < existing.toDouble()) {
              value.resetValue(summary, new_dp);
            }
            break;
          case 3:
            if (new_dp.toDouble() > existing.toDouble()) {
              value.resetValue(summary, new_dp);
            }
            break;
          case 5:
            // avg
            value.resetValue(summary, (new_dp.toDouble() + existing.toDouble()) / (double) 2);
            break;
          case 6:
            // first, skip
            break;
          case 7:
            // last
            value.resetValue(summary, new_dp);
          }
        }
      }
    }
  }
  
  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericSummaryType> next() {
    called = true;
    return value;
  }

  @Override
  public TypeToken<NumericSummaryType> getType() {
    return NumericSummaryType.TYPE;
  }

}
