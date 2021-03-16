package com.gaohan.hadoop.hdfs.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * 使用HDFS API完成词频统计
 *
 * 1） 读取带统计的数据
 * 2） 业务逻辑：词频统计 => Mapper
 * 3） 将结果存到某处 => Context
 * 4） 将结果写到某个地方 => HDFS API
 */
public class HDFSWCApp01 {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        URI uri = new URI("hdfs://192.168.44.12:8020");
        FileSystem fileSystem = FileSystem.get(uri, configuration, "root");

        WordCountMapper mapper = new WordCountMapper();
        HDFSContext context = new HDFSContext();

        // 1.读
        Path path = new Path("/hdfsapi/wc-bak.txt");

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            FSDataInputStream in = fileSystem.open(fileStatus.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                // System.out.println(line);
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }

        Path outputPath = new Path("/hdfsapi");
        FSDataOutputStream out = fileSystem.create(new Path(outputPath, new Path("wc.out")));

        Map<Object, Object> cacheMap = context.getCacheMap();
        for (Map.Entry<Object, Object> entry : cacheMap.entrySet()) {
            // System.out.println(entry.getKey() + "==>" + entry.getValue());
            out.write((entry.getKey().toString() + "\t" + entry.getValue().toString() + "\n").getBytes());
        }
        out.close();
        fileSystem.close();
    }
}
