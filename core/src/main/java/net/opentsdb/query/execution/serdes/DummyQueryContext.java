package net.opentsdb.query.execution.serdes;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.*;
import net.opentsdb.stats.Span;

import java.util.Collection;

public class DummyQueryContext implements QueryPipelineContext {

    protected TSDB tsdb;

    public DummyQueryContext(TSDB tsdb) {
        this.tsdb = tsdb;
    }

    @Override
    public TSDB tsdb() {
        return tsdb;
    }

    @Override
    public QueryPipelineContext pipelineContext() {
        return this;
    }


    @Override
    public TimeSeriesQuery query() {
        return null;
    }


    @Override
    public QueryContext queryContext() {
        return null;
    }


    @Override
    public Deferred<Void> initialize(final Span span) {
        return null;
    }


    @Override
    public void fetchNext(final Span span) { }


    @Override
    public Collection<QueryNode> upstream(final QueryNode node) {
        return null;
    }


    @Override
    public Collection<QueryNode> downstream(final QueryNode node) {
        return null;
    }


    @Override
    public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node) {
        return null;
    }


    @Override
    public Collection<String> downstreamSourcesIds(final QueryNode node) {
        return null;
    }


    @Override
    public Collection<QueryNode> upstreamOfType(final QueryNode node,
                                                final Class<? extends QueryNode> type) {
        return null;
    }


    @Override
    public Collection<QueryNode> downstreamOfType(final QueryNode node,
                                                  final Class<? extends QueryNode> type) {
        return null;
    }


    @Override
    public Collection<QuerySink> sinks() {
        return null;
    }


    @Override
    public void addId(final long hash, final TimeSeriesId id) { }


    @Override
    public TimeSeriesId getId(final long hash,
                              final TypeToken<? extends TimeSeriesId> type) {
        return null;
    }

    @Override
    public boolean hasId(final long hash,
                         final TypeToken<? extends TimeSeriesId> type) {
        return false;
    }

    @Override
    public void close() { }


    @Override
    public QueryNodeConfig config() {
        return null;
    }

    @Override
    public void onComplete(final QueryNode downstream,
                           final long final_sequence,
                           final long total_sequences) { }

    @Override
    public void onNext(final QueryResult next) { }

    @Override
    public void onNext(final PartialTimeSeries next) { }

    @Override
    public void onError(final Throwable t) { }

}
