package com.gaohan.hadoop.hdfs.wc;

public class WordCountMapper implements HDFSMapper{
    @Override
    public void map(String line, HDFSContext context) {
        String[] splits = line.split(",");
        for (String word : splits) {
            Object value = context.getCacheObject(word);
            if (value == null) {
                context.setCacheMap(word, 1);
            }else {
                int count = Integer.parseInt(value.toString());
                context.setCacheMap(word, count);
            }
        }
    }
}
