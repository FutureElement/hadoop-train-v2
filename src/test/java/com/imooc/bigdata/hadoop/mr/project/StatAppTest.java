package com.imooc.bigdata.hadoop.mr.project;

import com.imooc.bigdata.hadoop.hdfs.FileSystemFactory;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

public class StatAppTest {

    @Test
    public void test() throws IOException {
        final FileSystem instance1 = FileSystemFactory.getInstance();
        final FileSystem instance2 = FileSystemFactory.getInstance(true);
        assert !instance1.getConf().equals(instance2.getConf());
        instance1.close();
        instance2.close();
    }

}
