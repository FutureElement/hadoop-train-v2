package com.imooc.bigdata.hadoop.hdfs.mapper;

import com.imooc.bigdata.hadoop.hdfs.context.HDFSContext;

public interface Mapper {
    void map(String line, HDFSContext hdfsContext);
}
