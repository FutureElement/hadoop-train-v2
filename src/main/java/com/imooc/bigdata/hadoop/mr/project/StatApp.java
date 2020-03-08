package com.imooc.bigdata.hadoop.mr.project;

import com.imooc.bigdata.hadoop.hdfs.FileSystemFactory;
import com.imooc.bigdata.hadoop.mr.project.mr.PVStatApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StatApp {
    private Class<? extends Mapper<?, ?, ?, ?>> mapperClass = null;
    private Class<?> mapperOutputKeyClass = null;
    private Class<?> mapperOutputValueClass = null;

    private Class<? extends Reducer<?, ?, ?, ?>> reducerClass = null;
    private Class<?> outputKeyClass = null;
    private Class<?> outputValueClass = null;


    public void runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Path in = new Path(args[0]);
        final Path out = new Path(args[1]);

        final FileSystem fileSystem = FileSystemFactory.getInstance();
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        final Configuration configuration = FileSystemFactory.getConfiguration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(PVStatApp.class);

        if (mapperClass != null) {
            job.setMapperClass(mapperClass);
            job.setMapOutputKeyClass(mapperOutputKeyClass);
            job.setMapOutputValueClass(mapperOutputValueClass);
        }
        if (reducerClass != null) {
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(outputKeyClass);
            job.setOutputValueClass(outputValueClass);
        }
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.waitForCompletion(true);
    }

    public void setMapper(Class<? extends Mapper<?, ?, ?, ?>> mapperClass, Class<?> outputKeyClass, Class<?> outputValueClass) {
        this.mapperClass = mapperClass;
        this.mapperOutputKeyClass = outputKeyClass;
        this.mapperOutputValueClass = outputValueClass;
    }

    public void setReducer(Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<?> outputKeyClass, Class<?> outputValueClass) {
        this.reducerClass = reducerClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
    }

}
