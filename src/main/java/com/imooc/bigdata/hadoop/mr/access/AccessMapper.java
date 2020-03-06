package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] lines = value.toString().split("\t");

        final String phone = lines[1];
        final long up = Long.parseLong(lines[lines.length - 3]);
        final long down = Long.parseLong(lines[lines.length - 2]);

        context.write(new Text(phone), new Access(phone, up, down));
    }
}
