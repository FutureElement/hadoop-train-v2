package com.imooc.bigdata.hadoop.mr.project.mr;

import com.imooc.bigdata.hadoop.hdfs.FileSystemFactory;
import com.imooc.bigdata.hadoop.mr.project.StatApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PVStatApp extends StatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final ProvinceStatApp provinceStatApp = new ProvinceStatApp();
        provinceStatApp.setMapper(ProvinceStatApp.MyMapper.class, Text.class, LongWritable.class);
        provinceStatApp.setReducer(ProvinceStatApp.MyReducer.class, Text.class, LongWritable.class);
        provinceStatApp.runJob(args);
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
