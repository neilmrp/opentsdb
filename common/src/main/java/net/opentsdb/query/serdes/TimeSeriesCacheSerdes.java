package net.opentsdb.query.serdes;

import java.util.Collection;

import net.opentsdb.query.QueryResult;

public interface TimeSeriesCacheSerdes {

  public void serialize(final Collection<QueryResult> results, 
                        final CacheSerdesCallback callback);
  
  public static interface CacheSerdesCallback {
    
    public void onComplete(final byte[] data);
    
    public void onError(final Throwable t);
  }
  
}
