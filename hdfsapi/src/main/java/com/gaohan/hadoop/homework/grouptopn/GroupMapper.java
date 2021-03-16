package com.gaohan.hadoop.homework.grouptopn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, Hobby, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");

        Hobby hobby = new Hobby();
        hobby.setId(Integer.parseInt(line[0]));
        hobby.setThing(line[1]);
        hobby.setPrice(Double.parseDouble(line[2]));

        DoubleWritable price = new DoubleWritable(hobby.getPrice());

        context.write(hobby, price);
    }
}
