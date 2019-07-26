package net.opentsdb.query.serdes;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import net.opentsdb.query.CacheQueryResultFactory;
import net.opentsdb.query.QueryResult;

/**
 * A simple factory for the protobuf serialization and caching functionality.
 *
 */
public class PBufCacheResultFactory extends BaseTSDBPlugin implements CacheQueryResultFactory {
    public static final String TYPE = "PBufCacheResult";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
        this.tsdb = tsdb;
        this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
        return Deferred.fromResult(null);
    }

    public PBufCacheResult newSerializer(final QueryResult result, final SerdesOptions options) {
        if (result == null) {
            throw new IllegalArgumentException("Query Result to be cached cannot be null.");
        }
        return new PBufCacheResult(result, options);
    }
}
