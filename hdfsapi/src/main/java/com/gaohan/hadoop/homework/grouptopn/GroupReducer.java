package com.gaohan.hadoop.homework.grouptopn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<Hobby, DoubleWritable, Hobby, NullWritable> {

    @Override
    protected void reduce(Hobby key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int n = 0;
        for (DoubleWritable value: values) {
            n ++;
            if (n < 3) {
                context.write(key, NullWritable.get());
            } else {
                break;
            }
        }
    }
}
