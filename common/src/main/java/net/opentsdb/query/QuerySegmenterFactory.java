package net.opentsdb.query;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.serdes.SerdesOptions;

public interface QuerySegmenterFactory extends TSDBPlugin {

    public QuerySegmenter newSerializer(final QueryResult result, final SerdesOptions options);

}