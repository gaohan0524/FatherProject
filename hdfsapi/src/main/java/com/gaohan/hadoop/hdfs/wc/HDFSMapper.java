package com.gaohan.hadoop.hdfs.wc;

public interface HDFSMapper {

    public void map(String line, HDFSContext context);
}
