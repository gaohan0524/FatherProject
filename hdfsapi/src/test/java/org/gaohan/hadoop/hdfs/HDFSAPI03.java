package org.gaohan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSAPI03 {

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
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/join3.txt"));

        FileOutputStream out = new FileOutputStream(new File("out/join3.txt"));

        IOUtils.copyBytes(in, out, 4096);

        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        // source
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File("data/wc.data")));
        //dist
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/join3.txt"));

        IOUtils.copyBytes(in, out, 4096);

        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
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
