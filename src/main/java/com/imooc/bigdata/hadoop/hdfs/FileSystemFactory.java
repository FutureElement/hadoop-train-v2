package com.imooc.bigdata.hadoop.hdfs;

import com.imooc.bigdata.hadoop.hdfs.common.Constants;
import com.imooc.bigdata.hadoop.hdfs.common.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.util.Properties;

public class FileSystemFactory {
    private FileSystemFactory() {
    }

    public static FileSystem getInstance() {
        try {
            final Properties properties = PropertiesUtil.getProperties();
            final Configuration configuration = new Configuration();
            configuration.set(Constants.DFS_REPLICATION, properties.getProperty(Constants.DFS_REPLICATION));

            final String hdfsPath = properties.getProperty(Constants.DFS_PATH);
            final String hdfsUser = properties.getProperty(Constants.DFS_USER);

            return FileSystem.get(new URI(hdfsPath), configuration, hdfsUser);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
