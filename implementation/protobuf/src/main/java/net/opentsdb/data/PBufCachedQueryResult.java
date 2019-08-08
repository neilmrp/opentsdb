package net.opentsdb.data;

import com.google.common.reflect.TypeToken;
import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.TimeStampPB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.cache.QueryCachePlugin.CachedQueryResult;
import net.opentsdb.rollup.RollupConfig;

import java.time.temporal.ChronoUnit;
import java.util.Collection;

public class PBufCachedQueryResult implements CachedQueryResult {


    private PBufQueryResult result;
    private TimeStamp last;


    public PBufCachedQueryResult(PBufQueryResult result, TimeStamp last) {
        this.result = result;
        this.last = last;
    }
    public TimeStamp lastValueTimestamp() {
        return last;
    }


    @Override
    public TimeSpecification timeSpecification() {
        return result.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
        return result.timeSeries();
    }

    @Override
    public String error() {
        return "Exception encountered creating PBufCachedQueryResult";
    }

    @Override
    public Throwable exception() {
        return new Exception();
    }

    @Override
    public long sequenceId() {
        // TODO Auto-generated method stub
        return result.sequenceId();
    }

    @Override
    public QueryNode source() {
        return result.source();
    }

    @Override
    public String dataSource() {
        return result.dataSource();
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
        return Const.TS_STRING_ID;
    }

    @Override
    public ChronoUnit resolution() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RollupConfig rollupConfig() {
        return result.rollupConfig();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
