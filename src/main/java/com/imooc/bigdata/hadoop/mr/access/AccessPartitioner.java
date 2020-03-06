package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartitioner extends Partitioner<Text, Access> {
    @Override
    public int getPartition(Text key, Access value, int numPartitions) {
        final String phone = value.getPhone();
        if (phone.startsWith("13")) {
            return 0;
        } else if (phone.startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
