package com.imooc.bigdata.hadoop.mr.wc;

import com.imooc.bigdata.hadoop.hdfs.FileSystemFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceWordCountApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = Job.getInstance(FileSystemFactory.getConfiguration());
        job.setJarByClass(MapReduceWordCountApp.class);
        //mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        final FileSystem fileSystem = FileSystemFactory.getInstance();
        final Path output = new Path("/MapReduce/wc/output");
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/MapReduce/wc/input"));
        FileOutputFormat.setOutputPath(job, output);

        final boolean completion = job.waitForCompletion(true);

        System.exit(completion ? 0 : -1);
    }
}
