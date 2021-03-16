package com.gaohan.hadoop.hdfs.wc;

import java.util.HashMap;
import java.util.Map;

public class HDFSContext {
    // 定义一个缓存map
    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();
    // 得到整个map
    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }
    // 往map里写
    public void setCacheMap(Object key, Object value) {
        cacheMap.put(key, value);
    }
    // 根据key，获得对应value
    public Object getCacheObject(Object key) {
        return cacheMap.get(key);
    }
}
