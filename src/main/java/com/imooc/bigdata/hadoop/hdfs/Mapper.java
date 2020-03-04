package com.imooc.bigdata.hadoop.hdfs;

public interface Mapper {
    void map(String line, HDFSContext hdfsContext);
}
