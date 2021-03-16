package com.gaohan.hadoop.mr.wc;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用MR编程 完成词频统计
 */
public class WordCountDriverV2 {

    public static void main(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Configuration configuration = new Configuration();

        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 如果output目录存在，就删除
        FileUtils.deleteTargetedFile(configuration, output);

        // 2.设置class
        job.setJarByClass(WordCountDriverV2.class);

        // 3.设置Mapper和Reducer的class
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
