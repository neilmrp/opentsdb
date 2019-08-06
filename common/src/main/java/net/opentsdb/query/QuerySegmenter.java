package net.opentsdb.query;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.serdes.SerdesOptions;

import java.util.Collection;
import java.util.List;

public interface QuerySegmenter {

    /**
     * Encodes the given iterator.
     * @param result A query result.
     * @param blocksize Time duration of each block in seconds.
     * @return Collection of smaller QueryResults.
     */
    public List<QueryResult> segmentResult(final QueryResult result, final long blocksize);

}