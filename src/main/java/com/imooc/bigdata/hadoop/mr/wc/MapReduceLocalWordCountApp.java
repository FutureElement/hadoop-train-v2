package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceLocalWordCountApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = Job.getInstance();
        job.setJarByClass(MapReduceLocalWordCountApp.class);
        //mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        final boolean completion = job.waitForCompletion(true);

        System.exit(completion ? 0 : -1);
    }
}
