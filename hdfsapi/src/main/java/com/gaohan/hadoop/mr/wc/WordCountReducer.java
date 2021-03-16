package com.gaohan.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces a set of intermediate values which share a key to a smaller set of values.
 *
 * 相同key的数据都在一起吧
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("=============WordCountReducer.setup===========");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("============WordCountReducer.cleanup=============");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        System.out.println("===========WordCountReducer.reduce===============");

        // 对每个key做聚合操作（次数相加）
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
