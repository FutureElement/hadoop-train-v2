package com.imooc.bigdata.hadoop.mr.project.mrv2.local.mrv2;

import com.imooc.bigdata.hadoop.mr.project.LogData;
import com.imooc.bigdata.hadoop.mr.project.StatApp;
import com.imooc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLApp extends StatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final ETLApp etlApp = new ETLApp();
        etlApp.setMapper(MyMapper.class, NullWritable.class, Text.class);
        etlApp.runLocalJob(new String[]{"input/raw/trackinfo_20130721.txt", "input/etl"});
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
