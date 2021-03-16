package com.gaohan.hadoop.homework.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

/**
 * 使用Java操作HDFS API 实现
 * 作业1. 统计指定目录下[传几个大于blocksize的文件]，不足blocksize数据块的占比
 * 作业2. 统计指定目录下某个后辍名的文件个数、并删除
 */
public class HDFSByJavaApp {

    public static void main(String[] args) throws Exception {

        Path path1 = new Path("/hdfsapi/block");
        Path path2 = new Path("/hdfsapi/suffix");
        String suffix = "sh";

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        URI uri = new URI("hdfs://192.168.44.12:8020");
        FileSystem fileSystem = FileSystem.get(uri, configuration, "root");

        // 作业一：统计指定目录下，不足blocksize数据块的占比
        blockRatioCount(fileSystem, path1);

        // 作业二：统计指定目录下某个后辍名的文件个数、并删除
        suffixCountAndDel(fileSystem, suffix, path2);

    }

    public static void blockRatioCount(FileSystem fileSystem, Path path) throws IOException {

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);

        int blockCount = 0;
        int lackBlockCount = 0;

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // System.out.println(blockLocation);
                // System.out.println(blockLocation.getLength());
                if (blockLocation.getLength() < fileStatus.getBlockSize()) {
                    lackBlockCount ++;
                }
                blockCount ++;
            }
        }
        System.out.format("不足blocksize的块占比为：%d / %d", lackBlockCount, blockCount);
    }

    public static void suffixCountAndDel(FileSystem fileSystem, String suffix, Path path) throws IOException {

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);

        int suffixCount = 0;

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            // System.out.println(fileStatus.getPath().getName());
            if (fileStatus.getPath().getName().endsWith(suffix)) {
                fileSystem.delete(fileStatus.getPath(), true);
                suffixCount++;
            }
        }
        // System.out.println(suffixCount);
        System.out.format("后缀为%s的文件数为%d，并已删除", suffix, suffixCount);
    }

}
