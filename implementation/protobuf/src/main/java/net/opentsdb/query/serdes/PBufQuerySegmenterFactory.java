package net.opentsdb.query.serdes;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

/**
 * A simple factory for the protobuf serialization and caching functionality.
 *
 */
public class PBufQuerySegmenterFactory extends BaseTSDBPlugin implements TimeSeriesCacheSerdesFactory {
    public static final String TYPE = "PBufQuerySegmenterFactory";

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


    @Override
    public TimeSeriesCacheSerdes getSerdes() {
        return new PBufQuerySegmenter();
    }
}