package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class HDFSApp {

    private FileSystem fileSystem = null;

    @Before
    public void before() {
        final Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = HDFSUtil.getFileSystem(configuration);
    }

    @After
    public void after() throws IOException {
        fileSystem.close();
        fileSystem = null;
    }

    @Test
    public void mkdirs() throws IOException {
        final Path path = new Path("/hdfsapi/test");
        final boolean mkdirs = fileSystem.mkdirs(path);
        assert mkdirs;
    }

    @Test
    public void open() throws IOException {
        final Path path = new Path("/cdh_version.properties");
        final FSDataInputStream fsDataInputStream = fileSystem.open(path);
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        fsDataInputStream.close();
    }

    @Test
    public void create() throws IOException {
//        final Path path = new Path("/hdfsapi/test/a.txt");
        final Path path = new Path("/hdfsapi/test/b.txt");
        final FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        fsDataOutputStream.writeUTF("hello PK");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    @Test
    public void rename() throws IOException {
        final Path oldPath = new Path("/hdfsapi/test/b.txt");
        final Path newPath = new Path("/hdfsapi/test/c.txt");
        final boolean rename = fileSystem.rename(oldPath, newPath);
        assert rename;
    }

    @Test
    public void copyFromLocalFile() throws IOException {
        final Path src = new Path("/Users/gzhang/workspace/learn/hadoop-train-v2/src/main/resources/words.txt");
        final Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    @Test
    public void copyFromLocalBigFile() throws IOException {
        final InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/gzhang/Downloads/test.zip")));
        final Path dst = new Path("/hdfsapi/test/jdk.tgz");
        final FSDataOutputStream out = fileSystem.create(dst, () -> System.out.print("."));

        IOUtils.copyBytes(in, out, 1024);
    }

    @Test
    public void copyToLocalFile() throws IOException {
        final Path src = new Path("/hdfsapi/test/pom.xml");
        final Path dst = new Path("/Users/gzhang/workspace/learn/hadoop-train-v2/copy");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void listStatus() throws IOException {
        final Path src = new Path("/hdfsapi/test");
        final FileStatus[] fileStatuses = fileSystem.listStatus(src);
        Arrays.stream(fileStatuses).forEach(item -> System.out.println(item.toString()));
    }

    @Test
    public void listFilesRecursive() throws IOException {
        final Path src = new Path("/hdfsapi");
        final RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(src, true);
        while (listFiles.hasNext()) {
            System.out.println(listFiles.next().toString());
        }
    }

    @Test
    public void getFileBlockLocations() throws IOException {
        final Path path = new Path("/hdfsapi/test/jdk.tgz");
        final FileStatus fileStatus = fileSystem.getFileStatus(path);
        final BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        Arrays.stream(fileBlockLocations).forEach(it -> System.out.println(it.toString()));
    }

    @Test
    public void delete() throws IOException {
        final Path path = new Path("/hdfsapi/test/jdk.tgz");
        final boolean delete = fileSystem.delete(path, false);
        assert delete;
    }
}
