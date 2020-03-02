package com.imooc.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class HDFSUtil {
    private HDFSUtil() {
    }

    public static FileSystem getFileSystem(String hdfsPath, Configuration configuration, String user) {
        try {
            return FileSystem.get(new URI(hdfsPath), configuration, user);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
