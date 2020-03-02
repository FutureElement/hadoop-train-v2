package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;

public interface HDFSConf {
    public static final String PATH = "hdfs://hadoop000:8020";
    public static final Configuration CONFIGURATION = new Configuration();
    public static final String USER = "gzhang";
}
