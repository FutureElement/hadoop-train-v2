package com.imooc.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

public class HDFSContext {

    private final Map<String, Integer> contextMap = new HashMap<>();

    public Map<String, Integer> getContextMap() {
        return contextMap;
    }

    public void write(String key, int count) {
        contextMap.put(key, count);
    }

    public Integer get(String key) {
        return contextMap.get(key);
    }

}
