package net.opentsdb.query.execution.cache;

import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

public interface QueryReadCache {

  public QueryResult fetch(final QueryPipelineContext context);
  
}
