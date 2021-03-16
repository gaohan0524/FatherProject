package com.gaohan.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 删除target目录
 */
public class FileUtils {

    public static void deleteTargetedFile(Configuration configuration, String target) throws Exception{
        FileSystem fileSystem = FileSystem.get(configuration);

        Path path = new Path(target);

        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
}
