package net.opentsdb.query.execution.cache;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.core.Response;

import com.google.common.collect.Lists;
import net.opentsdb.data.*;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Callback;

import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.serdes.SerdesCallback;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
//import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
//import net.opentsdb.servlet.exceptions.QueryExecutionExceptionMapper;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;


/**
 * A simple sink that will serialize the results.
 *
 * @since 3.0
 */
public class CacheSink implements QuerySink, SerdesCallback {
    private static final Logger LOG = LoggerFactory.getLogger(
            CacheSink.class);
    private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");

    /** The context we're operating under. */
    private final QueryContext context;

    /** The sink config. */
    private final CacheSinkConfig config;

    /** The serializer. */
    private final TimeSeriesSerdes serdes;

    /** TEMP - This sucks but we need to figure out proper async writes. */
    private final ByteArrayOutputStream stream;

    /** The query sink callback. Only one allowed. */
    private QuerySinkCallback callback;

//    private final String pbufplugin;

    /**
     * Default ctor.
     * @param context The non-null context.
     * @param config The non-null config.
     */
    public CacheSink(final QueryContext context,
                     final CacheSinkConfig config) {
        this.context = context;
        this.config = config;

//        System.out.println(config.serdesOptions().getType());

        // serdesOptions here should be a PBufSerdesOptions
        final SerdesFactory factory = context.tsdb().getRegistry()
                .getPlugin(SerdesFactory.class, config.serdesOptions().getType());

        if (factory == null) {
            throw new IllegalArgumentException("Unable to find a serdes "
                    + "factory for the type: " + config.serdesOptions().getType());
        }

        // TODO - noooo!!!!
        stream = new ByteArrayOutputStream();

        // this serdes should be a PBufSerdes instance
//        serdes = context.tsdb().getRegistry().getPlugin()

        serdes = factory.newInstance(
                context,
                config.serdesOptions(),
                stream);
//        System.out.println(serdes.getClass());
        if (serdes == null) {
            throw new IllegalArgumentException("Factory returned a null "
                    + "instance for the type: " + config.serdesOptions().getType());
        }
//        pbufplugin = context.tsdb().getRegistry().getPlugin()
    }

    @Override
    public void onComplete() {
        if (context.query().isTraceEnabled()) {
            context.logTrace("Query serialization complete.");
        }
        if (context.query().getMode() == QueryMode.VALIDATE) {
            // no-op
            return;
        }
        try {
            serdes.serializeComplete(null);
            config.request().setAttribute("DATA", stream);
            if (context.stats() != null) {
                context.stats().incrementSerializedDataSize(stream.size());
            }
//      try {
//        // TODO - oh this is sooooo ugly.... *sniff*
//        config.response().setContentType("application/json");
//        final byte[] data = stream.toByteArray();
//        stream.close();
//        config.response().setContentLength(data.length);
//        config.response().setStatus(200);
//        config.response().getOutputStream().write(data);
//        config.response().getOutputStream().close();
//      } catch (IOException e1) {
//        onError(e1);
//        return;
//      }
            //config.async().complete();
//            System.out.println("******* cache async dispatching");
            config.async().dispatch();
            logComplete();
        } catch (Exception e) {
            LOG.error("Unexpected exception dispatching async request for "
                    + "query: " + JSON.serializeToString(context.query()), e);
//            GenericExceptionMapper.serialize(e, config.async().getResponse());
            config.async().complete();
            logComplete();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Yay, all done!");
        }
    }

    @Override
    public void onNext(final QueryResult next) {
        if (next.exception() != null) {
            onError(next.exception());
            return;
        }

        if (next.error() != null) {
            onError(new QueryExecutionException(next.error(), 0));
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Successful response for query="
                    + JSON.serializeToString(
                    ImmutableMap.<String, Object>builder()
                            // TODO - possible upstream headers
                            .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
                            .put("node", next.source().config().getId() + ":" + next.dataSource())
                            .build()));
        }
        if (context.query().isTraceEnabled()) {
            context.logTrace(next.source(), "Received response: "
                    + next.source().config().getId() + ":" + next.dataSource());
        }

        final Span serdes_span = context.stats().querySpan() != null ?
                context.stats().querySpan().newChild("onNext_"
                        + next.source().config().getId() + ":" + next.dataSource())
                        .start()
                : null;

        class FinalCB implements Callback<Void, Object> {
            @Override
            public Void call(final Object ignored) throws Exception {
                if (ignored != null && ignored instanceof Throwable) {
                    LOG.error("Failed to serialize result: "
                            + next.source().config().getId() + ":"
                            + next.dataSource(), (Throwable) ignored);
                }
                try {
                    next.close();
                } catch (Throwable t) {
                    LOG.warn("Failed to close result: "
                            + next.source().config().getId() + ":" + next.dataSource(), t);
                }
                if (context.query().isTraceEnabled()) {
                    context.logTrace(next.source(), "Finished serializing response: "
                            + next.source().config().getId() + ":" + next.dataSource());
                }
                if (serdes_span != null) {
                    serdes_span.setSuccessTags().finish();
                }
                return null;
            }
        }

        try {
            // TODO: split query result into sub query result blocks
            // have to figure out whether to serialize by one hour or one day blocks

            final long blocksize = 3600;



            final CacheQueryResultFactory cacheSegmentFactory = context.tsdb().getRegistry()
                    .getPlugin(CacheQueryResultFactory.class, null);

//            System.out.println("******************");
//            System.out.println(cacheSegmentFactory);

//            System.out.println("******************");
//            System.out.println("starting segmentation");
//            System.out.println(context.query().startTime().epoch() + " " + context.query().endTime().epoch());
//
            System.out.println("*******Display Original Result*******");
            displayQueryResult(next, true);


            CacheQueryResult cacheSegmenter = cacheSegmentFactory.newSerializer(next, config.serdesOptions());
            List<QueryResult> segmentedResults = cacheSegmenter.segmentResult(next, blocksize);

            for (int i = 0; i < segmentedResults.size(); i++) {
                System.out.println("****Segment " + (i+1) + " ******");
                displayQueryResult(segmentedResults.get(i), true);
            }

//             have to implement serialization
            for (QueryResult segment : segmentedResults) {
                serdes.serialize(segment, serdes_span)
                        .addBoth(new FinalCB());
            }

//            serdes.serialize(next, serdes_span)
//                    .addBoth(new FinalCB());


        } catch (Exception e) {
            onError(e);
            return;
        }

    }

    public void displayQueryResult(QueryResult res, boolean displayValues) {
        int count = 0;
        for (TimeSeries ts : res.timeSeries()) {
            count++;
            System.out.println("START NEW TIME SERIES " +  count);
            for (TypedTimeSeriesIterator it : ts.iterators()) {
                System.out.println("Starting new iterator");
                long startTime = 0;
                long endTime = 0;
                int numValues = 0;
                boolean setStartTime = false;
                if (it.getType().equals(NumericType.TYPE)) {
                    while (it.hasNext()) {
                        TimeSeriesValue<NumericType> val = (TimeSeriesValue<NumericType>) it.next();

                        String displayVal = val.value().toString().contains("NaN") ? "NaN" : val.value().longValue() + "";
                        if (displayValues) {
                            System.out.println(val.timestamp().epoch() + " " + displayVal);
                        }
                        numValues++;
                        startTime = !setStartTime ? val.timestamp().epoch() : startTime;
                        setStartTime = true;
                        endTime = val.timestamp().epoch();
                    }
                }
                System.out.println("Start: " + startTime + " End: " + endTime + " NumValues: " + numValues);
            }
        }
//        System.out.println("TS Count " + count);
    }

    @Override
    public void onNext(final PartialTimeSeries next,
                       final QuerySinkCallback callback) {
        if (this.callback == null) {
            synchronized (this) {
                if (this.callback == null) {
                    this.callback = callback;
                }
            }
        }

        if (this.callback != callback) {
            // TODO WTF?
        }
        serdes.serialize(next, this, null /** TODO */);
    }

    @Override
    public void onError(final Throwable t) {
        LOG.error("Exception for query: "
                + JSON.serializeToString(context.query()), t);
        context.logError("Error sent to the query sink: " + t.getMessage());
        try {
//            if (t instanceof QueryExecutionException) {
//                QueryExecutionExceptionMapper.serialize(
//                        (QueryExecutionException) t,
//                        config.async().getResponse());
//            } else {
//                GenericExceptionMapper.serialize(t, config.async().getResponse());
//            }
            config.async().complete();
            logComplete(t);
        } catch (Throwable t1) {
            LOG.error("WFT? response may have been serialized?", t1);
        }
    }

    void logComplete() {
        logComplete(null);
    }

    void logComplete(final Throwable t) {
        if (config.statsTimer() != null) {
            try {
                // extract a namespace if possible.
                String namespace = null;
                for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
                    if (config instanceof TimeSeriesDataSourceConfig) {
                        String local_namespace = ((TimeSeriesDataSourceConfig) config).getNamespace();
                        if (Strings.isNullOrEmpty(local_namespace)) {
                            // extract from metric filter.
                            local_namespace = ((TimeSeriesDataSourceConfig) config).getMetric().getMetric();
                            local_namespace = local_namespace.substring(0, local_namespace.indexOf('.'));
                        }

                        if (namespace == null) {
                            namespace = local_namespace;
                        } else {
                            if (!namespace.equals(local_namespace)) {
                                // Different namespaces so we won't use one.
                                namespace = "MultipleNameSpaces";
                                break;
                            }
                        }
                    }
                }

                config.statsTimer().stop("user", context.authState() != null ?
                                context.authState().getUser() : "Unkown",
                        "namespace", namespace,
                        "endpoint", config.request().getRequestURI() /* TODO - trim */);
            } catch (Exception e) {
                LOG.error("Failed to record timer", e);
            }
        }

        if (context.stats() != null) {
            context.stats().emitStats();
        }

        LOG.info("Completing query="
                + JSON.serializeToString(ImmutableMap.<String, Object>builder()
                // TODO - possible upstream headers
                .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
                .put("user", context.authState() != null ? context.authState().getPrincipal().getName() : "Unkown")
                .put("traceId", context.stats().trace() != null ?
                        context.stats().trace().traceId() : "")
                .put("status", Response.Status.OK)
                .put("error", t == null ? "null" : t.toString())
                .put("statRDS", context.stats() == null ? 0 : context.stats().rawDataSize())
                .put("statRTS", context.stats() == null ? 0 : context.stats().rawTimeSeriesCount())
                .put("statSDS", context.stats() == null ? 0 : context.stats().serializedDataSize())
                .put("statSTS", context.stats() == null ? 0 : context.stats().serializedTimeSeriesCount())
                .build()));

        QUERY_LOG.info("Completing query="
                + JSON.serializeToString(ImmutableMap.<String, Object>builder()
                // TODO - possible upstream headers
                .put("queryId", Bytes.byteArrayToString(context.query().buildHashCode().asBytes()))
                .put("user", context.authState() != null ? context.authState().getPrincipal().getName() : "Unkown")
                //.put("queryHash", Bytes.byteArrayToString(context.query().buildTimelessHashCode().asBytes()))
                .put("traceId", context.stats().trace() != null ?
                        context.stats().trace().traceId() : "")
                .put("status", Response.Status.OK)
                //.put("trace", trace.serializeToString())
                .put("query", JSON.serializeToString(context.query()))
                .put("error", t == null ? "null" : t.toString())
                .build()));

        if (context.stats().querySpan() != null) {
            if (t != null) {
                context.stats().querySpan()
                        .setErrorTags(t)
                        .setTag("query", JSON.serializeToString(context.query()))
                        .finish();
            } else {
                context.stats().querySpan()
                        .setSuccessTags()
                        .setTag("query", JSON.serializeToString(context.query()))
                        .finish();
            }
        }
    }

    @Override
    public void onComplete(final PartialTimeSeries pts) {
        callback.onComplete(pts);
    }

    @Override
    public void onError(final PartialTimeSeries pts, final Throwable t) {
        callback.onError(pts, t);
    }
}

