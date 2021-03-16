package com.gaohan.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Maps input key/value pairs to a set of intermediate key/value pairs.
 * LongWritable: 输入的key类型
 * Text：输入的value类型
 *
 * Text: 拆解开的每个单词
 * IntWritable: 1
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable ONE = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("==========WordCountMapper.setup==========");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("==========WordCountMapper.cleanup==========");
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("==========WordCountMapper.map==========");

        // 获取到内容
        String line = value.toString();

        // 按照分隔符进行拆分
        String[] splits = line.split(",");

        // 输出
        for (String word : splits) {
            context.write(new Text(word), ONE);
        }
    }
}
