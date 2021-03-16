package com.gaohan.hadoop.mr.ser;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;

        for (Access access : values) {
            ups += access.getUp();
            downs += access.getDown();
        }

        Access access = new Access();
        access.setPhone(key.toString());
        access.setUp(ups);
        access.setDown(downs);
        access.setSum(ups + downs);

        context.write(NullWritable.get(), access);
    }
}
