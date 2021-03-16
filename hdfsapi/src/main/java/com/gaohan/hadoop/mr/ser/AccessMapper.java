package com.gaohan.hadoop.mr.ser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String phone = splits[1]; // 第二个字段：手机号
        long up = Long.parseLong(splits[splits.length - 3]);
        long down = Long.parseLong(splits[splits.length - 2]);

        Access access = new Access();
        access.setPhone(phone);
        access.setUp(up);
        access.setDown(down);
        access.setSum(up + down);

        context.write(new Text(phone), access);
    }
}
