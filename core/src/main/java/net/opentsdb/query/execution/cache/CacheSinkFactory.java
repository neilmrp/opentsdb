package net.opentsdb.query.execution.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkConfig;
import net.opentsdb.query.QuerySinkFactory;

/**
 * A factory to generate the cache sink.
 *
 * @since 3.0
 */
public class CacheSinkFactory extends BaseTSDBPlugin
        implements QuerySinkFactory {

    public static final String TYPE = "TSDBCacheSink";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
        this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
        return Deferred.fromResult(null);
    }

    @Override
    public QuerySink newSink(final QueryContext context,
                             final QuerySinkConfig config) {
        return new CacheSink(context, (CacheSinkConfig) config);
    }

    @Override
    public String version() {
        return "3.0.0";
    }

    @Override
    public QuerySinkConfig parseConfig(final ObjectMapper mapper,
                                       final TSDB tsdb,
                                       final JsonNode node) {
        try {
            return mapper.treeToValue(node, CacheSinkConfig.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to parse JSON", e);
        }
    }
}

