package net.opentsdb.query;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.serdes.SerdesOptions;

public interface CacheQueryResultFactory extends TSDBPlugin {

    public CacheQueryResult newSerializer(final QueryResult result, final SerdesOptions options);

}
