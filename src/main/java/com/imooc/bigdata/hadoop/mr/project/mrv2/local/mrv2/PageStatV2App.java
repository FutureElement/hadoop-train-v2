package com.imooc.bigdata.hadoop.mr.project.mrv2.local.mrv2;

import com.imooc.bigdata.hadoop.mr.project.LogData;
import com.imooc.bigdata.hadoop.mr.project.StatApp;
import com.imooc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageStatV2App extends StatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final PageStatV2App statV2App = new PageStatV2App();
        statV2App.setMapper(MyMapper.class, Text.class, LongWritable.class);
        statV2App.setReducer(MyReducer.class, Text.class, LongWritable.class);
        statV2App.runLocalJob(new String[]{"input/etl", "output/v2/pagestat"});
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) {
            logParser = LogParser.getInstance();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final LogData logData = logParser.parseAllV2(value.toString());
            context.write(new Text(logData.getPageId()), ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }
}
