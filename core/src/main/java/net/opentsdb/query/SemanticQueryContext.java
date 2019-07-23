// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query;

import java.util.List;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;

public class SemanticQueryContext extends BaseQueryContext {
  
  protected SemanticQueryContext(final Builder builder) {
    super((BaseQueryContext.Builder) builder);
    pipeline = new LocalPipeline(this, builder.sinks);
  }
  
  
  /**
   * Simple pipeline implementation.
   */
  protected class LocalPipeline extends AbstractQueryPipelineContext {

    public LocalPipeline(final QueryContext context, 
                         final List<QuerySink> direct_sinks) {
      super(context);
      if (direct_sinks != null && !direct_sinks.isEmpty()) {
        sinks.addAll(direct_sinks);
      }
    }

    @Override
    public Deferred<Void> initialize(final Span span) {
      final Span child;
      if (span != null) {
        child = span.newChild(getClass().getSimpleName() + ".initialize()")
                     .start();
      } else {
        child = null;
      }
      
      class SpanCB implements Callback<Void, Void> {
        @Override
        public Void call(final Void ignored) throws Exception {
          if (child != null) {
            child.setSuccessTags().finish();
          }
          return null;
        }
      }
      
      return initializeGraph(child).addCallback(new SpanCB());
    }
    
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseQueryContext.Builder {
   
    @Override
    public QueryContext build() {
      return (QueryContext) new SemanticQueryContext(this);
    }
    
  }

}
