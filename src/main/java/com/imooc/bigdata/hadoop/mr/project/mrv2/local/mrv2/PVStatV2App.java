package com.imooc.bigdata.hadoop.mr.project.mrv2.local.mrv2;

import com.imooc.bigdata.hadoop.mr.project.StatApp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PVStatV2App extends StatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final PVStatV2App statV2App = new PVStatV2App();
        statV2App.setMapper(MyMapper.class, Text.class, LongWritable.class);
        statV2App.setReducer(MyReducer.class, Text.class, LongWritable.class);
        statV2App.runLocalJob(new String[]{"input/etl", "output/v2/pvstat"});
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text KEY = new Text("key");
        private static final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY, ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable ignored : values) {
                count++;
            }
            context.write(NullWritable.get(), new LongWritable(count));
        }
    }
}
