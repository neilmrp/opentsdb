package net.opentsdb.query.serdes;

import java.util.Collection;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryResult;

public interface TimeSeriesCacheSerdes {

  public byte[] serialize(final Collection<QueryResult> results);

  public Map<String, QueryResult> deserialize(final byte[] data);
}
