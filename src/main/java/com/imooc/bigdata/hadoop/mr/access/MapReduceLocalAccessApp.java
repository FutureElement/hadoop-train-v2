package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceLocalAccessApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = Job.getInstance();
        job.setJarByClass(MapReduceLocalAccessApp.class);
        //mapper
        job.setMapperClass(AccessMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        //reducer
        job.setReducerClass(AccessReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        //partitioner
        job.setPartitionerClass(AccessPartitioner.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("access/input"));
        FileOutputFormat.setOutputPath(job, new Path("access/output_" + System.currentTimeMillis()));

        final boolean completion = job.waitForCompletion(true);

        System.exit(completion ? 0 : -1);
    }
}
