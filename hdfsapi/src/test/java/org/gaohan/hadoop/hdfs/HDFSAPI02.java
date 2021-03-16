package org.gaohan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSAPI02 {

    FileSystem fileSystem;
    Configuration configuration;

    @Before
    public void setUp() throws Exception{
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        URI uri = new URI("hdfs://192.168.44.12:8020");
        fileSystem = FileSystem.get(uri, configuration, "root");
    }

    @After
    public void tearDown() throws IOException {
        fileSystem.close();
    }

    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/test"), true);
    }

    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi"), true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            boolean isDir = fileStatus.isDirectory();
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void rename() throws IOException {
        Path src = new Path("/hdfsapi/wc.data");
        Path dst = new Path("/hdfsapi/wc-bak.txt");
        fileSystem.rename(src, dst);
    }

    @Test
    public void copyToLocalFile() throws IOException {
        Path src = new Path("/hdfsapi/wc.data");
        Path dst = new Path("out/wc.txt");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void copyFromLocalFile() throws Exception {

        Path src = new Path("data/wc.data");
        Path dst = new Path("/hdfsapi/");
        fileSystem.copyFromLocalFile(src, dst);

    }

    @Test
    public void mkdir() throws Exception {

        // 对文件系统做相应操作
        Path path = new Path("/hdfsapi");
        boolean isSucceeded = fileSystem.mkdirs(path);

        // 断言
        Assert.assertEquals(true, isSucceeded);
    }
}
