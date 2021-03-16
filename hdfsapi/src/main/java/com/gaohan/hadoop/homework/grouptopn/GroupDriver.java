package com.gaohan.hadoop.homework.grouptopn;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * >>>>>>>>>>>求分组TopN>>>>>>>>>>>
 * 输入数据：
 * 11,皮鞭,10
 * 11,蜡烛,3
 * 11,骰子,1
 * 11,扑克牌,2
 * 1,Spark,80
 * 1,Flink,60
 * 1,CDH,60
 * 1,Hadoop,50
 * 27,农夫山泉,2
 * 27,Galaxy Z Flip,20999
 * 27,Mate Xs,18666
 * 求每个人最喜欢的两大爱好的金额：
 * 11	10.0
 * 11	3.0
 * 1	80.0
 * 1	60.0
 * 27	20999.0
 * 27	18666.0
 */
public class GroupDriver {

    public static void main(String[] args) throws Exception {
        String input = "data/grouptopn.txt";
        String output = "out";

        Configuration configuration = new Configuration();
        //configuration.setInt("topn", topN);

        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 如果output目录存在，就删除
        FileUtils.deleteTargetedFile(configuration, output);

        // 2.设置class
        job.setJarByClass(GroupDriver.class);

        // 3.设置Mapper和Reducer的class
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        // 4.设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Hobby.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // 5.设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Hobby.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(MyGroupComparator.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
