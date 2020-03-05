package com.imooc.bigdata.hadoop.hdfs;


import com.imooc.bigdata.hadoop.hdfs.common.Constants;
import com.imooc.bigdata.hadoop.hdfs.common.PropertiesUtil;
import com.imooc.bigdata.hadoop.hdfs.context.HDFSContext;
import com.imooc.bigdata.hadoop.hdfs.mapper.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class HDFSWordCountApp {
    public static void main(String[] args) {
        final Properties properties = PropertiesUtil.getProperties();

        final Path in = new Path(properties.getProperty(Constants.WC_INPUT));
        final Path out = new Path(properties.getProperty(Constants.WC_OUTPUT));
        final HDFSContext hdfsContext = new HDFSContext();
        try (
                final FileSystem fs = FileSystemFactory.getInstance();
                final FSDataInputStream fsDataInputStream = fs.open(in);
                final FSDataOutputStream fsDataOutputStream = fs.create(out);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream))
        ) {
            final Mapper mapper = (Mapper) Class.forName(properties.getProperty(Constants.WC_MAPPER))
                    .getDeclaredConstructor().newInstance();
            String line;
            while ((line = reader.readLine()) != null) {
                mapper.map(line, hdfsContext);
            }

            hdfsContext.getContextMap().forEach((k, v) -> {
                try {
                    fsDataOutputStream.writeUTF(k + " : " + v + "\n");
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            });
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            hdfsContext.clear();
        }
    }
}
