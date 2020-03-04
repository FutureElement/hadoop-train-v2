package com.imooc.bigdata.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HDFSWordCountApp01 {
    public static void main(String[] args) {
        final Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");

        final HDFSContext hdfsContext = new HDFSContext();
        final Mapper wordCountMapper = new WordCountMapper();

        final Path in = new Path("/hdfsapi/test/words.txt");
        final Path out = new Path("/hdfsapi/output/result.txt");

        try (
                final FileSystem fs = HDFSUtil.getFileSystem(configuration);
                final FSDataInputStream fsDataInputStream = fs.open(in);
                final FSDataOutputStream fsDataOutputStream = fs.create(out);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                wordCountMapper.map(line, hdfsContext);
            }

            hdfsContext.getContextMap().forEach((k, v) -> {
                try {
                    fsDataOutputStream.writeUTF(k + " : " + v + "\n");
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            });


        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }


    }
}
