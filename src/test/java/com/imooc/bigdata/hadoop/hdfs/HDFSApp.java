package com.imooc.bigdata.hadoop.hdfs;

import com.imooc.bigdata.HDFSUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFSApp {

    private FileSystem fileSystem = null;

    @Before
    public void before() {
        fileSystem = HDFSUtil.getFileSystem(
                HDFSConf.PATH,
                HDFSConf.CONFIGURATION,
                HDFSConf.USER
        );
    }

    @Test
    public void test_mkdirs() throws IOException {
        final Path path = new Path("/hdfsapi/test");
        final boolean mkdirs = fileSystem.mkdirs(path);
        assert mkdirs;
    }
}
