package com.hubspot.hbase.tasks.helpers.jmx;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

class RegionServerJMXPoolFactory extends BaseKeyedPoolableObjectFactory<String, RegionServerJMXHostInfo> {
  public RegionServerJMXPoolFactory() {  }

  @Override
  public RegionServerJMXHostInfo makeObject(final String key) throws Exception {
    return new RegionServerJMXHostInfo(key);
  }

  @Override
  public boolean validateObject(final String key, final RegionServerJMXHostInfo obj) {
    return obj.isAlive();
  }

  @Override
  public void destroyObject(final String key, final RegionServerJMXHostInfo obj) throws Exception {
    if (obj != null) {
      obj.close();
    }
  }

  public static KeyedObjectPool<String, RegionServerJMXHostInfo> getInstance() {
    final GenericKeyedObjectPool<String, RegionServerJMXHostInfo> pool = new GenericKeyedObjectPool<String, RegionServerJMXHostInfo>(new RegionServerJMXPoolFactory());
    pool.setTestOnBorrow(true);
    pool.setTestOnReturn(false);
    pool.setTestWhileIdle(true);
    pool.setMaxIdle(50);
    pool.setMaxActive(1);
    pool.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
    return pool;
  }

}
