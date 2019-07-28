package net.opentsdb.query.serdes;

import net.opentsdb.core.TSDBPlugin;

public interface TimeSeriesCacheSerdesFactory extends TSDBPlugin {

  public TimeSeriesCacheSerdes getSerdes();
  
}
