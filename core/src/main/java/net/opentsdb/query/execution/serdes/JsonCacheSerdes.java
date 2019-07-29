package net.opentsdb.query.execution.serdes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventType;
import net.opentsdb.data.types.event.EventsGroupValue;
import net.opentsdb.data.types.event.EventsValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.status.StatusIterator;
import net.opentsdb.data.types.status.StatusType;
import net.opentsdb.data.types.status.StatusValue;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.summarizer.Summarizer;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdes;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdesFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

public class JsonCacheSerdes implements TimeSeriesCacheSerdes, TimeSeriesCacheSerdesFactory {

  @Override
  public byte[] serialize(Collection<QueryResult> results) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(baos);
      json.writeStartArray();
      
      for (final QueryResult result : results) {
        // TODO make sure ids are strings.
        json.writeStartObject();
        json.writeStringField("source", result.source().config().getId() + ":" + result.dataSource());
        // TODO - array of data sources

        // serdes time spec if present
        if (result.timeSpecification() != null) {
          json.writeObjectFieldStart("timeSpecification");
          // TODO - ms, second, nanos, etc
          json.writeNumberField("start", result.timeSpecification().start().epoch());
          json.writeNumberField("end", result.timeSpecification().end().epoch());
          json.writeStringField("intervalISO", result.timeSpecification().interval() != null ?
              result.timeSpecification().interval().toString() : "null");
          json.writeStringField("interval", result.timeSpecification().stringInterval());
          //json.writeNumberField("intervalNumeric", result.timeSpecification().interval().get(result.timeSpecification().units()));
          if (result.timeSpecification().timezone() != null) {
            json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
          }
          json.writeStringField("units", result.timeSpecification().units() != null ?
              result.timeSpecification().units().toString() : "null");
          json.writeEndObject();
        }

        json.writeArrayFieldStart("data");
        int idx = 0;

        boolean wasStatus = false;
        boolean wasEvent =false;
        String namespace = null;
        
        for (final TimeSeries series : result.timeSeries()) {
          serializeSeries(series, 
              (TimeSeriesStringId) series.id(),
              json,
              null,
              result);
          if (!wasStatus ) {
            for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator :
                series.iterators()) {
              if (iterator.getType() == StatusType.TYPE) {
                namespace = ((StatusIterator) iterator).namespace();
                wasStatus = true;
                break;
              }
            }
          }

        }
        // end of the data array
        json.writeEndArray();

        if(wasStatus && null != namespace && !namespace.isEmpty()) {
          json.writeStringField("namespace", namespace);
        }

        json.writeEndObject();
      }
      
      json.writeEndArray();
      json.close();
    } catch (IOException e1) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e1);
    }
    System.out.println("           [SERIALIZED] " + new String(baos.toByteArray()));
    
    return baos.toByteArray();
  }

  @Override
  public Map<String, QueryResult> deserialize(byte[] data) {
    System.out.println("------------- DESER: " + Bytes.pretty(data));
    Map<String, QueryResult> map = Maps.newHashMap();
    try {
      JsonNode results = JSON.getMapper().readTree(data);
    
      for (final JsonNode result : results) {
        QueryResult r = new HttpQueryV3Result(null, result, null);
        map.put(r.source().config().getId() + ":" + r.dataSource(), r);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return map;
  }
  
  private void serializeSeries(
      final TimeSeries series,
      final TimeSeriesStringId id,
      JsonGenerator json,
      final List<String> sets,
      final QueryResult result) throws IOException {

//    final ByteArrayOutputStream baos;
//    if (json == null) {
//      baos = new ByteArrayOutputStream();
//      json = JSON.getFactory().createGenerator(baos);
//    } else {
//      baos = null;
//    }

    boolean wrote_values = false;
    boolean was_status = false;
    boolean was_event = false;
    boolean was_event_group = false;
    for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator : series.iterators()) {
      while (iterator.hasNext()) {
        TimeSeriesValue<? extends TimeSeriesDataType> value = iterator.next();
        if (iterator.getType() == StatusType.TYPE) {
          if (!was_status) {
            was_status = true;
          }
          json.writeStartObject();
          writeStatus((StatusValue) value, json);
          wrote_values = true;
        } else if (iterator.getType() == EventType.TYPE) {
          was_event = true;
          json.writeStartObject();
          json.writeObjectFieldStart("EventsType");
          writeEvents((EventsValue) value, json);
          json.writeEndObject();
          wrote_values = true;
        } else if (iterator.getType() == EventGroupType.TYPE) {
          was_event_group = true;
          json.writeStartObject();
          writeEventGroup((EventsGroupValue) value, json, id);
          wrote_values = true;
        } else {
//          while (value != null && value.timestamp().compare(Op.LT, start)) {
//            if (iterator.hasNext()) {
//              value = iterator.next();
//            } else {
//              value = null;
//            }
//          }
//
//          if (value == null) {
//            continue;
//          }
//          if (value.timestamp().compare(Op.LT, start) || value.timestamp().compare(Op.GT, end)) {
//            continue;
//          }

          if (iterator.getType() == NumericType.TYPE) {
            if (writeNumeric((TimeSeriesValue<NumericType>) value, iterator, json, result, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericSummaryType.TYPE) {
            if (writeNumericSummary(value, iterator, json, result, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericArrayType.TYPE) {
            if(writeNumericArray((TimeSeriesValue<NumericArrayType>) value, iterator, json, result, wrote_values)) {
              wrote_values = true;
            }
          }
        }
      }
    }

    if (wrote_values) {
      // serialize the ID
      if(!was_status && !was_event) {
        json.writeStringField("metric", id.metric());
      }
      if (! was_event_group) {
        json.writeObjectFieldStart("tags");
        for (final Entry<String, String> entry : id.tags().entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeEndObject();
      }
      if (was_event) {
        json.writeNumberField("hits", id.hits());
      } else {
        json.writeArrayFieldStart("aggregateTags");
        for (final String tag : id.aggregatedTags()) {
          json.writeString(tag);
        }
        json.writeEndArray();
      }
      json.writeEndObject();
    }

//    if (baos != null) {
//      json.close();
//      synchronized(sets) {
//        sets.add(new String(baos.toByteArray(), Const.UTF8_CHARSET));
//      }
//      baos.close();
//    } else {
//      json.flush();
//    }
  }

  private boolean writeNumeric(
      TimeSeriesValue<NumericType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {
    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      // just the values
      while (value != null) {
        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
          wrote_type = true;
        }

        if (value.value() == null) {
          json.writeNull();
        } else {
          if ((value).value().isInteger()) {
            json.writeNumber(
                (value).value().longValue());
          } else {
            json.writeNumber(
                (value).value().doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      return wrote_type;
    }

    // timestamp and values
    boolean wrote_local = false;
    while (value != null) {
      long ts = value.timestamp().epoch();
      final String ts_string = Long.toString(ts);

      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeObjectFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if ((value).value().isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().longValue());
        } else {
          json.writeNumberField(ts_string,
              (value).value().doubleValue());
        }
      }

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      } else {
        value = null;
      }
      wrote_local = true;
    }
    if (wrote_local) {
      json.writeEndObject();
    }
    return wrote_type;
  }

  private boolean writeRollupNumeric(
      TimeSeriesValue<NumericSummaryType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      Collection<Integer> summaries = null;
      Integer summary = null;

      // just the values
      while (value != null) {
        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
          wrote_type = true;
        }

        if (value.value() == null) {
          //TODO, should we use json.writeNull() instead?
          json.writeNumber(Double.NaN);
        } else {

          // Will fetch summaries from the first non null dps.
          if (summaries == null) {
            summaries =
                (value).value().summariesAvailable();
            summary = summaries.iterator().next();
          }
          if ((value).value().value(summary).isInteger()) {
            json.writeNumber(
                (value).value().value(summary).longValue());
          } else {
            json.writeNumber(
                (value).value().value(summary).doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      return wrote_type;
    }

    Collection<Integer> summaries = null;
    Integer summary = null;

    // timestamp and values
    while (value != null) {
      long ts = value.timestamp().epoch();
      final String ts_string = Long.toString(ts);

      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeObjectFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }

      if (summaries == null) {
        summaries =
            (value).value().summariesAvailable();
        summary = summaries.iterator().next();
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if ((value).value().value(summary).isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().value(summary).longValue());
        } else {
          json.writeNumberField(ts_string,
              (value).value().value(summary).doubleValue());
        }
      }

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      } else {
        value = null;
      }
    }
    json.writeEndObject();
    return wrote_type;
  }

  private boolean writeNumericSummary(
      TimeSeriesValue value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      if (!(result.source() instanceof Summarizer)) {
        return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value, iterator, json,
            result, wrote_values);
      }

      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value)
              .value()
              .summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        long ts = value.timestamp().epoch();

        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeObjectFieldStart("NumericSummaryType");
          json.writeArrayFieldStart("aggregations");
          for (final int summary : summaries) {
            json.writeString(result.rollupConfig().getAggregatorForId(summary));
          }
          json.writeEndArray();

          json.writeArrayFieldStart("data");
          wrote_type = true;
        }

        if (value.value() == null) {
          json.writeNull();
        } else {
          final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
          json.writeStartArray();
          for (final int summary : summaries) {
            final NumericType summary_value = v.value(summary);
            if (summary_value == null) {
              json.writeNull();
            } else if (summary_value.isInteger()) {
              json.writeNumber(summary_value.longValue());
            } else {
              json.writeNumber(summary_value.doubleValue());
            }
          }
          json.writeEndArray();
        }

        if (iterator.hasNext()) {
          value = iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      json.writeEndObject();
      return wrote_type;
    }

    // NOTE: This is assuming all values have the same summaries available.

    // Rollups result would typically be a groupby and not a summarizer
    if (!(result.source() instanceof Summarizer)) {
      return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value,
          iterator, json, result, wrote_values);
    }

    if (((TimeSeriesValue<NumericSummaryType>) value).value() != null) {
      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        long ts = value.timestamp().epoch();
        final String ts_string = Long.toString(ts);

        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeObjectFieldStart("NumericSummaryType");
          json.writeArrayFieldStart("aggregations");
          for (final int summary : summaries) {
            json.writeString(result.rollupConfig().getAggregatorForId(summary));
          }
          json.writeEndArray();

          json.writeArrayFieldStart("data");
          wrote_type = true;
        }
        if (value.value() == null) {
          json.writeNullField(ts_string);
        } else {
          json.writeStartObject();
          final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
          json.writeArrayFieldStart(ts_string);
          for (final int summary : summaries) {
            final NumericType summary_value = v.value(summary);
            if (summary_value == null) {
              json.writeNull();
            } else if (summary_value.isInteger()) {
              json.writeNumber(summary_value.longValue());
            } else {
              json.writeNumber(summary_value.doubleValue());
            }
          }
          json.writeEndArray();
          json.writeEndObject();
        }

        if (iterator.hasNext()) {
          value = iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      json.writeEndObject();
    }
    return wrote_type;
  }

  private boolean writeNumericArray(
      TimeSeriesValue<NumericArrayType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {

    if (value.value().end() < 1) {
      // no data
      return false;
    }

    // we can assume here that we have a time spec as we can't get arrays
    // without it.
    boolean wrote_type = false;
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }
      if (value.value().isInteger()) {
        json.writeNumber(value.value().longArray()[i]);
      } else {
        json.writeNumber(value.value().doubleArray()[i]);
      }
    }
    json.writeEndArray();
    return wrote_type;
  }

  private void writeEventGroup(EventsGroupValue eventsGroupValue, final JsonGenerator json,
      final TimeSeriesStringId id)
      throws IOException {
    json.writeObjectFieldStart("EventsGroupType");
    if (eventsGroupValue.group() != null) {
      json.writeObjectFieldStart("group");
      for (Map.Entry<String, String> e : eventsGroupValue.group().entrySet()) {
        json.writeStringField(e.getKey(), String.valueOf(e.getValue()));
      }
      json.writeEndObject();
    }

    json.writeObjectFieldStart("event");
    writeEvents(eventsGroupValue.event(), json);
    json.writeObjectFieldStart("tags");
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue());
    }
    json.writeEndObject();
    json.writeEndObject();

    json.writeEndObject();
  }
  private void writeEvents(EventsValue eventsValue, final JsonGenerator json) throws IOException {
    json.writeStringField("namespace", eventsValue.namespace());
    json.writeStringField("source", eventsValue.source());
    json.writeStringField("title", eventsValue.title());
    json.writeStringField("message", eventsValue.message());
    json.writeStringField("priority", eventsValue.priority());
    json.writeStringField("timestamp", Long.toString(eventsValue.timestamp().epoch()));
    json.writeStringField("endTimestamp", Long.toString(eventsValue.endTimestamp().epoch()));
    json.writeStringField("userId", eventsValue.userId());
    json.writeBooleanField("ongoing", eventsValue.ongoing());
    json.writeStringField("eventId", eventsValue.eventId());
    if (eventsValue.parentId() != null) {
      json.writeArrayFieldStart("parentId");
      for (String p : eventsValue.parentId()) {
        json.writeString(p);
      }
    }
    json.writeEndArray();
    if (eventsValue.childId() != null) {
      json.writeArrayFieldStart("childId");
      for (String c : eventsValue.childId()) {
        json.writeString(c);
      }
    }
    json.writeEndArray();

    if (eventsValue.additionalProps() != null) {
      json.writeObjectFieldStart("additionalProps");
      for (Map.Entry<String, Object> e : eventsValue.additionalProps().entrySet()) {
        json.writeStringField(e.getKey(), String.valueOf(e.getValue()));
      }
      json.writeEndObject();
    }

  }
  
  private void writeStatus(StatusValue statusValue, final JsonGenerator json) throws IOException {

    byte[] statusCodeArray = statusValue.statusCodeArray();
    if (null == statusCodeArray) {
      json.writeNumberField("statusCode", statusValue.statusCode());
    } else {
      json.writeArrayFieldStart("statusCodeArray");
      for (byte code : statusCodeArray) {
        json.writeNumber(code);
      }
      json.writeEndArray();
    }

    TimeStamp[] timeStampArray = statusValue.timestampArray();
    if (null == timeStampArray) {
      json.writeNumberField("lastUpdateTime", statusValue.lastUpdateTime().msEpoch());
    } else {
      json.writeArrayFieldStart("timestampArray");
      for (TimeStamp timeStamp : timeStampArray) {
        json.writeNumber(timeStamp.epoch());
      }
      json.writeEndArray();
    }

    json.writeNumberField("statusType", statusValue.statusType());
    json.writeStringField("message", statusValue.message());
    json.writeStringField("application", statusValue.application());
  }

  
  public class HttpQueryV3Result implements QueryResult {

    /** The node that owns us. */
    private QueryNode node;
    
    /** The name of this data source. */
    private String data_source;
    
    /** The time spec parsed out. */
    private TimeSpecification time_spec;
    
    /** The list of series we found. */
    private List<TimeSeries> series;
    
    /** An optional exception. */
    private Exception exception;
    
    /** An optional rollup config from summaries. */
    private RollupConfig rollup_config;

    /** Object Mapper for serdes. */
    private final ObjectMapper mapper = new ObjectMapper();
    
    /**
     * Default ctor without an exception.
     * @param node The non-null parent node.
     * @param root The non-null root node.
     */
    HttpQueryV3Result(final QueryNode node, 
                      final JsonNode root, 
                      final RollupConfig rollup_config) {
      this(node, root, rollup_config, null);
    }
    
    /**
     * Ctor with an exception. If the exception isn't null then the root 
     * must be set.
     * @param node The non-null parent node.
     * @param root The root node. Cannot be null if the exception is null.
     * @param exception An optional exception.
     */
    HttpQueryV3Result(final QueryNode node, 
                      final JsonNode root, 
                      final RollupConfig rollup_config,
                      final Exception exception) {
      this.node = node;
      this.exception = exception;
      this.rollup_config = rollup_config;
      if (exception == null) {
        String temp = root.get("source").asText();
        data_source = temp.substring(temp.indexOf(":") + 1);
        if (this.node == null) {
          this.node = new DummyQueryNode(temp.substring(0, temp.indexOf(":")));
        }
        
        JsonNode n = root.get("timeSpecification");
        if (n != null && !n.isNull()) {
          time_spec = new TimeSpec(n);
        }
        
        n = root.get("data");
        if (n != null && !n.isNull()) {
          series = Lists.newArrayList();
          int i = 0;
          for (final JsonNode ts : n) {
            series.add(new HttpTimeSeries(ts));
            if (i++ == 0) {
              // check for numerics
              JsonNode rollups = ts.get("NumericSummaryType");
              if (rollup_config == null) {
                if (rollups != null && !rollups.isNull()) {
                  this.rollup_config = new RollupData(ts);
                } else {
                  this.rollup_config = null;
                }
              }
            }
          }
        } else {
          series = Collections.emptyList();
        }
      } else {
        series = Collections.emptyList();
      }
    }

    @Override
    public TimeSpecification timeSpecification() {
      return time_spec;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return series;
    }

    @Override
    public String error() {
      return exception != null ? exception.getMessage() : null;
    }
    
    @Override
    public Throwable exception() {
      return exception;
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
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RollupConfig rollupConfig() {
      return rollup_config;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }
    
    class TimeSpec implements TimeSpecification {

      private final TimeStamp start;
      private final TimeStamp end;
      private final String string_interval;
      private final TemporalAmount interval;
      private final ChronoUnit units;
      private final ZoneId time_zone;
      
      TimeSpec(final JsonNode node) {
        start = new SecondTimeStamp(node.get("start").asLong());
        end = new SecondTimeStamp(node.get("end").asLong());
        string_interval = node.get("interval").asText();
        if (string_interval.toLowerCase().equals("0all")) {
          interval = null;
        } else {
          interval = DateTime.parseDuration2(string_interval);
        }
        // TODO - get the proper units.
        //units = ChronoUnit(node.get("units").asText());
        units = null;
        time_zone = ZoneId.of(node.get("timeZone").asText());
      }
      
      @Override
      public TimeStamp start() {
        return start;
      }

      @Override
      public TimeStamp end() {
        return end;
      }

      @Override
      public TemporalAmount interval() {
        return interval;
      }

      @Override
      public String stringInterval() {
        return string_interval;
      }

      @Override
      public ChronoUnit units() {
        return units;
      }

      @Override
      public ZoneId timezone() {
        return time_zone;
      }

      @Override
      public void updateTimestamp(int offset, TimeStamp timestamp) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void nextTimestamp(TimeStamp timestamp) {
        // TODO Auto-generated method stub
        
      }
      
    }

    /**
     * The base time series that parses out the ID and sets the root node
     * of the data.
     */
    class HttpTimeSeries implements TimeSeries {

      /** The parsed ID. */
      private final TimeSeriesStringId id;
      
      /** The time series root node. */
      private final JsonNode node;
      
      /** The parsed out types. */
      private final List<TypeToken<? extends TimeSeriesDataType>> types;
      
      /**
       * Default ctor.
       * @param node The non-null time series root node.
       */
      HttpTimeSeries(final JsonNode node) {
        this.node = node;
        final BaseTimeSeriesStringId.Builder builder =
            BaseTimeSeriesStringId.newBuilder();
        JsonNode temp = node.get("metric");
        if (temp != null && !temp.isNull()) {
          builder.setMetric(temp.asText());
        }
        temp = node.get("tags");
        if (temp != null && !temp.isNull()) {
          final Iterator<Entry<String, JsonNode>> iterator = temp.fields();
          while (iterator.hasNext()) {
            final Entry<String, JsonNode> entry = iterator.next();
            builder.addTags(entry.getKey(), entry.getValue().asText());
          }
        }

        temp = node.get("hits");
        if (temp != null && !temp.isNull()) {
          builder.setHits(temp.asLong());
        }

        temp = node.get("aggregateTags");
        if (temp != null && !temp.isNull()) {
          for (final JsonNode tag : temp) {
            builder.addAggregatedTag(tag.asText());
          }
        }
        id = builder.build();

        types = Lists.newArrayList();
        temp = node.get("NumericType");
        if (temp != null && !temp.isNull()) {
          if (time_spec != null) {
            types.add(NumericArrayType.TYPE);
          } else {
            types.add(NumericType.TYPE);
          }
        }

        temp = node.get("NumericSummaryType");
        if (temp != null && !temp.isNull()) {
          types.add(NumericSummaryType.TYPE);
        }

        temp = node.get("EventsType");
        if (temp != null && !temp.isNull()) {
          types.add(EventType.TYPE);
        }

        temp = node.get("EventsGroupType");
        if (temp != null && !temp.isNull()) {
          types.add(EventGroupType.TYPE);
        }
      }
      
      @Override
      public TimeSeriesId id() {
        return id;
      }

      @Override
      public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
          final TypeToken<? extends TimeSeriesDataType> type) {
        // TODO - cleanup
        if (types.contains(type)) {
          TypedTimeSeriesIterator<? extends TimeSeriesDataType> data = null;
          if (type == NumericType.TYPE) {
            data = new NumericData(node.get("NumericType"));
          } else if (type == NumericArrayType.TYPE) {
            data = new ArrayData(node.get("NumericType"));
          } else if (type == NumericSummaryType.TYPE) {
            data = new SummaryData(node.get("NumericSummaryType"));
          } else if (type == EventType.TYPE) {
            data = new EventData(node.get("EventsType"));
          } else if (type == EventGroupType.TYPE) {
            data = new EventGroupData(node.get("EventsGroupType"));
          }
          if (data != null) {
            return Optional.of(data);
          }
          return Optional.empty();
        }
        return Optional.empty();
      }

      @Override
      public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
        // TODO - cleanup
        List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> results = Lists
            .newArrayListWithExpectedSize(1);
        if (types.contains(NumericType.TYPE)) {
          results.add(new NumericData(node.get("NumericType")));
        } else if (types.contains(NumericArrayType.TYPE)) {
          results.add(new ArrayData(node.get("NumericType")));
        } else if (types.contains(EventType.TYPE)) {
          results.add(new EventData(node.get("EventsType")));
        } else if (types.contains(EventGroupType.TYPE)) {
          results.add(new EventGroupData(node.get("EventsGroupType")));
        }
        return results;
      }

      @Override
      public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
        return types;
      }

      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }
      
    }

    /**
     * Iterator for the NumericType.
     */
    class NumericData implements TypedTimeSeriesIterator {
      
      /** The data point we populate each time. */
      private MutableNumericValue dp;
      
      /** The iterator over the data points. */
      private Iterator<Entry<String, JsonNode>> iterator;
      
      /** The timestamp populated each round. */
      private TimeStamp timestamp;
      
      NumericData(final JsonNode data) {
        iterator = data.fields();
        timestamp = new SecondTimeStamp(0);
        dp = new MutableNumericValue();
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return NumericType.TYPE;
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> next() {
        Entry<String, JsonNode> entry = iterator.next();
        timestamp.updateEpoch(Long.parseLong(entry.getKey()));
        if (entry.getValue().isDouble()) {
          dp.reset(timestamp, entry.getValue().asDouble());
        } else {
          dp.reset(timestamp, entry.getValue().asLong());
        }
        return dp;
      }
      
    }
    
    /**
     * Implementation for the NumericArrayType.
     */
    class ArrayData implements TypedTimeSeriesIterator, 
        TimeSeriesValue<NumericArrayType>,
        NumericArrayType{

      /** The data arrays we'll populate. */
      private long[] long_data;
      private double[] double_data;
      
      /** Whether or not we were read. */
      private boolean was_read = false;
      
      /** The write index into the arrays. */
      private int idx = 0;
      
      ArrayData(final JsonNode data) {
        long_data = new long[data.size()];
        for (final JsonNode node : data) {
          if (node.isDouble() || node.isTextual() && 
              node.asText().toLowerCase().equals("nan")) {
            // TODO - infinites?
            if (double_data == null) {
              double_data = new double[long_data.length];
              for (int i = 0; i < idx; i++) {
                double_data[i] = long_data[i];
              }
              long_data = null;
            }
            
            if (idx >= double_data.length) {
              double[] temp = new double[idx < 1024 ? idx * 2 : idx + 8];
              for (int i = 0; i < idx; i++) {
                temp[i] = double_data[i];
              }
            }
            double_data[idx++] = node.asDouble();
          } else {
            if (long_data == null) {
              if (idx >= double_data.length) {
                double[] temp = new double[idx < 1024 ? idx * 2 : idx + 8];
                for (int i = 0; i < idx; i++) {
                  temp[i] = double_data[i];
                }
              }
              double_data[idx++] = node.asDouble();
            } else {
              if (idx >= long_data.length) {
                long[] temp = new long[idx < 1024 ? idx * 2 : idx + 8];
                for (int i = 0; i < idx; i++) {
                  temp[i] = long_data[i];
                }
              }
              long_data[idx++] = node.asLong();
            }
          }
        }
      }
      
      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return NumericArrayType.TYPE;
      }

      @Override
      public boolean hasNext() {
        return !was_read;
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> next() {
        was_read = true;
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return time_spec.start();
      }

      @Override
      public NumericArrayType value() {
        return this;
      }

      @Override
      public TypeToken<NumericArrayType> type() {
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
        return long_data != null;
      }

      @Override
      public long[] longArray() {
        return long_data;
      }

      @Override
      public double[] doubleArray() {
        return double_data;
      }
      
    }

    class SummaryData implements TypedTimeSeriesIterator {
      
      /** The data point we populate each time. */
      private MutableNumericSummaryValue dp;
      
      /** The timestamp populated each round. */
      private TimeStamp timestamp;
      
      private Iterator<JsonNode> iterator;
      
      SummaryData(final JsonNode data) {
        iterator = data.get("data").iterator();
        timestamp = new SecondTimeStamp(0);
        dp = new MutableNumericSummaryValue();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> next() {
        final JsonNode node = iterator.next();
        final Entry<String, JsonNode> entry = node.fields().next();
        timestamp.updateEpoch(Long.parseLong(entry.getKey()));
        dp.resetTimestamp(timestamp);
        
        int i = 0;
        for (final JsonNode agg : entry.getValue()) {
          if (agg.isDouble()) {
            dp.resetValue(i++, agg.asDouble());
          } else {
            dp.resetValue(i++, agg.asLong());
          }
        }
        return dp;
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return NumericSummaryType.TYPE;
      }
      
    }

    class EventData implements TypedTimeSeriesIterator {

      private JsonNode node;

      EventData(final JsonNode data) {
        node = data;
      }

      @Override
      public boolean hasNext() {
        return node == null ? false : true;
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> next() {

        EventsValue events_value;
        try {
          events_value  = mapper.treeToValue(node, EventsValue.class);
          node = null;
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Unable to parse config", e);
        }
        return events_value;
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return EventType.TYPE;
      }

    }

    class EventGroupData implements TypedTimeSeriesIterator {

      private JsonNode node;

      EventGroupData(final JsonNode data) {
        node = data;
      }

      @Override
      public boolean hasNext() {
        return node == null ? false : true;
      }

      @Override
      public TimeSeriesValue<? extends TimeSeriesDataType> next() {

        EventsGroupValue events_group_value;
        try {
          events_group_value = mapper.treeToValue(node, EventsGroupValue.class);
          node = null;
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Unable to parse config", e);
        }
        return events_group_value;
      }

      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return EventGroupType.TYPE;
      }

    }
    
    /**
     * A parsed rollup config.
     */
    class RollupData implements RollupConfig {
      
      /** Forward and reverse maps. */
      private Map<String, Integer> name_to_id;
      private Map<Integer, String> id_to_name;
      
      RollupData(JsonNode node) {
        name_to_id = Maps.newHashMap();
        id_to_name = Maps.newHashMap();
        
        // from "time series" object root.
        node = node.get("NumericSummaryType");
        if (node == null || node.isNull()) {
          return;
        }
        
        node = node.get("aggregations");
        int i = 0;
        for (final JsonNode agg : node) {
          name_to_id.put(agg.asText(), i);
          id_to_name.put(i++, agg.asText());
        }
      }
      
      @Override
      public Map<String, Integer> getAggregationIds() {
        return name_to_id;
      }

      @Override
      public String getAggregatorForId(final int id) {
        return id_to_name.get(id);
      }

      @Override
      public int getIdForAggregator(final String aggregator) {
        return name_to_id.get(aggregator);
      }

      @Override
      public List<String> getIntervals() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<String> getPossibleIntervals(String interval) {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
  }


  @Override
  public String type() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    // TODO Auto-generated method stub
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    // TODO Auto-generated method stub
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeSeriesCacheSerdes getSerdes() {
    return this;
  }
}
