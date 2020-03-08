package com.imooc.bigdata.hadoop.mr.project.mrv2;

import com.imooc.bigdata.hadoop.mr.project.LogData;
import com.imooc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Path in = new Path("input/raw/trackinfo_20130721.txt");
        final Path out = new Path("input/etl");

        final Configuration configuration = new Configuration();
        final FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        final Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLApp.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private LogParser logParser;

        @Override
        protected void setup(Context context) {
            logParser = LogParser.getInstance();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final LogData logData = logParser.parseAll(value.toString());
            if (logData != null) {
                String builder = "" +
                        logData.getIpInfo().getIp() + "\t" +
                        logData.getIpInfo().getCountry() + "\t" +
                        logData.getIpInfo().getProvince() + "\t" +
                        logData.getIpInfo().getCity() + "\t" +
                        logData.getUrl() + "\t" +
                        logData.getTime() + "\t" +
                        logData.getSessionId() + "\t" +
                        logData.getPageId();
                context.write(NullWritable.get(), new Text(builder));
            }
        }
    }
}
