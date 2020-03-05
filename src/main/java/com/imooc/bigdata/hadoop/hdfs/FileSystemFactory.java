package com.imooc.bigdata.hadoop.hdfs;

import com.imooc.bigdata.hadoop.hdfs.common.Constants;
import com.imooc.bigdata.hadoop.hdfs.common.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.util.Properties;

public class FileSystemFactory {

    private static Properties properties;

    static {
        properties = PropertiesUtil.getProperties();
        System.setProperty(Constants.HADOOP_USER_NAME, FileSystemFactory.getUser());
    }

    private FileSystemFactory() {
    }

    public static FileSystem getInstance() {
        try {
            return FileSystem.get(new URI(getPath()), getConfiguration(), getUser());
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Configuration getConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(Constants.DFS_REPLICATION, properties.getProperty(Constants.DFS_REPLICATION));
        configuration.set(Constants.DFS_DEFAULT_FS, getPath());
        return configuration;
    }

    public static String getUser() {
        return properties.getProperty(Constants.DFS_USER);
    }

    public static String getPath() {
        return properties.getProperty(Constants.DFS_PATH);
    }
}
