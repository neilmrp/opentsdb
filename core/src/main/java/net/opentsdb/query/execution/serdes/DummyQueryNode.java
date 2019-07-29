package net.opentsdb.query.execution.serdes;

import java.util.List;
import java.util.Map;

import com.google.common.hash.HashCode;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

public class DummyQueryNode implements QueryNode, QueryNodeConfig {

  private final String id;
  public DummyQueryNode(final String id) {
    this.id = id;
  }
  
  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List getSources() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean joins() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Map getOverrides() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getString(Configuration config, String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getInt(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getBoolean(Configuration config, String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public double getDouble(Configuration config, String key) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean hasKey(String key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Builder toBuilder() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryPipelineContext pipelineContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred initialize(Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryNodeConfig config() {
    return this;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(QueryResult next) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(PartialTimeSeries next) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onError(Throwable t) {
    // TODO Auto-generated method stub
    
  }

}
