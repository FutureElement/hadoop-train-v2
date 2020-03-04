package com.imooc.bigdata.hadoop.hdfs;

import java.util.Arrays;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String line, HDFSContext hdfsContext) {
        final String[] words = line.split(" ");
        Arrays.stream(words).forEach(word -> {
            Integer count = hdfsContext.get(word);
            if (count == null) {
                hdfsContext.write(word, 1);
            } else {
                hdfsContext.write(word, ++count);
            }
        });
    }
}
