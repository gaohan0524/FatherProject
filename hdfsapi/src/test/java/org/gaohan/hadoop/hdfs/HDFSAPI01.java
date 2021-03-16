package org.gaohan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSAPI01 {


    @Test
    public void copyFromLocalFile() throws Exception {
        // System.setProperty("HADOOP_USER_NAME", "root")

        Configuration configration = new Configuration();
        configration.set("dfs.replication", "1");
        URI uri = new URI("hdfs://192.168.44.12:8020");
        FileSystem fileSystem = FileSystem.get(uri, configration, "root");

        Path src = new Path("data/wc.data");
        Path dst = new Path("/hdfsapi/");
        fileSystem.copyFromLocalFile(src, dst);

        fileSystem.close();
    }

    @Test
    public void mkdir() throws Exception {
        Configuration configration = new Configuration();

        URI uri = new URI("hdfs://192.168.44.12:8020");
        // 获取文件系统客户端对象
        FileSystem fileSystem = FileSystem.get(uri, configration, "root");

        // 对文件系统做相应操作
        Path path = new Path("/hdfsapi");
        boolean isSucceeded = fileSystem.mkdirs(path);

        // 断言
        Assert.assertEquals(true, isSucceeded);

        // 关闭
        fileSystem.close();

    }
}
