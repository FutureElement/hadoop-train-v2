package com.imooc.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class HDFSUtil {
    private HDFSUtil() {
    }

    public static FileSystem getFileSystem(Configuration configuration) {
        try {
            return FileSystem.get(new URI(HDFSConf.PATH), configuration, HDFSConf.USER);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
