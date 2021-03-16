package com.gaohan.hadoop.homework.topn;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * >>>>>>>>>>>求TopN>>>>>>>>>>>
 * 输入数据：
 * 矿泉水,1
 * 扑克牌,3
 * 手铐,4
 * 骰子,5
 * 皮鞭,8
 * 蜡烛,8
 * 求班长最喜欢的三大爱好：
 * 蜡烛	8
 * 皮鞭	8
 * 骰子	5
 */
public class TopNDriver {

    // 也可以从args读进来
    private static int topN = 3;

    public static void main(String[] args) throws Exception {
        String input = "data/topNdata.txt";
        String output = "out";

        Configuration configuration = new Configuration();
        //configuration.setInt("topn", topN);

        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 如果output目录存在，就删除
        FileUtils.deleteTargetedFile(configuration, output);

        // 2.设置class
        job.setJarByClass(TopNDriver.class);

        // 3.设置Mapper和Reducer的class
        job.setMapperClass(MonitorMapper.class);
        job.setReducerClass(MonitorReducer.class);

        // 4.设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Monitor.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5.设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Monitor.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MonitorMapper extends Mapper<LongWritable, Text, Monitor, NullWritable> {

        Monitor monitor = new Monitor();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            monitor.setThing(line[0]);
            monitor.setNum(Integer.parseInt(line[1]));
            context.write(monitor, NullWritable.get());
        }
    }

    public static class MonitorReducer extends Reducer<Monitor, NullWritable, Monitor, NullWritable> {

        //private static int topN = 0;

//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            topN = context.getConfiguration().getInt("topn", topN);
//
//        }

        @Override
        protected void reduce(Monitor key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if (topN > 0) {
                context.write(key, NullWritable.get());
                topN --;
            }
        }
    }

}
