package com.gaohan.hadoop.mr.ser;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AccessDriver {


    public static void main(String[] args) throws Exception {
        String input = "data/access.log";
        String output = "out";

        Configuration configuration = new Configuration();

        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 如果output目录存在，就删除
        FileUtils.deleteTargetedFile(configuration, output);

        // 2.设置class
        job.setJarByClass(AccessDriver.class);

        // 3.设置Mapper和Reducer的class
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // 4.设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        // 5.设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
