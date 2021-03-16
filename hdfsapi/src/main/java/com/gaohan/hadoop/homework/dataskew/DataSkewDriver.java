package com.gaohan.hadoop.homework.dataskew;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * 需求：
 *  1. 两个MR作业：1打散 2聚合
 *  2. topN和分组topN 合成一个 串起来
 *
 * 打散倾斜原理：
 *  1、获取NumReduceTasks的个数，并将其随机
 *  2、在map结果的返回值中，将随机数拼接到key上。
 *  3、得出的结果再重新进行mapreduce计算，将后缀切掉，重新聚合。
 *  4、二次job时必须： //【重点】 设置reduce task 任务数 - 否则数据又倾斜了
 */
public class DataSkewDriver {

    //分隔符
    public static final String SPLIT_CHAR = "\001";
    private static final String inPath = DataSkewDriver.class.getResource("/data/ruoze_bigdata.dat").getPath();
    private static final String firOutPath = "D:/home/wc/out";
    private static final String finalOutPath = "D:/home/wc/finPath";

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        FileUtils.deleteTargetedFile(configuration,firOutPath);
        FileUtils.deleteTargetedFile(configuration,finalOutPath);

        //----------------------- 第一个Job任务
        Job firJob = Job.getInstance(configuration,"job1");

        firJob.setJarByClass(DataSkewDriver.class);
        firJob.setMapperClass(SecondJobFirMapper.class);
        firJob.setReducerClass(SecondJobFirReducer.class);

        firJob.setMapOutputKeyClass(Text.class);
        firJob.setMapOutputValueClass(LongWritable.class);
        firJob.setOutputKeyClass(Text.class);
        firJob.setOutputValueClass(LongWritable.class);

        //设置maptask端的局部聚合逻辑类 - 减少io - 减少reducer压力 [调优必须]
        firJob.setCombinerClass(SecondJobFirReducer.class);

        //设置reduce task 任务数
        firJob.setNumReduceTasks(3);

        //job控制器1
        ControlledJob ctrljob1 = new ControlledJob(configuration);
        ctrljob1.setJob(firJob);

        FileInputFormat.addInputPath(firJob,new Path(inPath));
        FileOutputFormat.setOutputPath(firJob,new Path(firOutPath));


        //----------------------- 第二个Job任务
        Job secJob = Job.getInstance(configuration,"job2");

        secJob.setJarByClass(DataSkewDriver.class);
        secJob.setMapperClass(SecondJobSecMapper.class);
        secJob.setReducerClass(SecondJobSecReducer.class);

        secJob.setMapOutputKeyClass(Text.class);
        secJob.setMapOutputValueClass(LongWritable.class);
        secJob.setOutputKeyClass(Text.class);
        secJob.setOutputValueClass(LongWritable.class);

        //设置maptask端的局部聚合逻辑类 - 减少io - 减少reducer压力 [调优必须]
        secJob.setCombinerClass(SecondJobSecReducer.class);

        //【重点】 设置reduce task 任务数 - 否则数据又倾斜了！
        secJob.setNumReduceTasks(1);

        //job控制器2
        ControlledJob ctrljob2 = new ControlledJob(configuration);
        ctrljob2.setJob(secJob);

        //设置多个作业直接的依赖关系  --- 串联 多个Job
        //【重点】  ctrljob2 的启动，依赖于ctrljob1作业的完成
        ctrljob2.addDependingJob(ctrljob1);

        //输入路径是上一个作业的输出路径  【此处有坑 ———— 必须使用通配符 ，将Job1中的输出进行指定到文件，否则报错！】
        FileInputFormat.addInputPath(secJob,new Path(firOutPath+"/part-r*"));
        FileOutputFormat.setOutputPath(secJob,new Path(finalOutPath));

        //主的控制容器，控制上面的总的两个子作业
        JobControl jobControl = new JobControl("CountjobCtrl");
        //添加到总的JobControl里，进行控制
        jobControl.addJob(ctrljob1);
        jobControl.addJob(ctrljob2);

        //在线程里启动： 主控制器 [必须]
        Thread t = new Thread(jobControl);
        t.start();

        //阻塞主线程 ， 直到第二个job执行完成
        while(true){
            if(jobControl.allFinished()){
                //全部Job执行完成，打印成功作业的信息
                System.out.println(jobControl.getSuccessfulJobList());

                System.out.println(jobControl.getFailedJobList());

                jobControl.stop();
                break;
            }
        }
    }

}

//第一个Job - Mapper
class SecondJobFirMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    private int numReduceTasks = 0;
    private final Random r = new Random();
    private final Text k = new Text();
    private final LongWritable v = new LongWritable(1);

    @Override
    protected void setup(Context context) {
        //获取 reduce task 的数量
        numReduceTasks = context.getNumReduceTasks();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        for (String word : words) {
            k.set(word + DataSkewDriver.SPLIT_CHAR + r.nextInt(numReduceTasks));
            context.write(k,v);
        }
    }
}

//第一个Job - Reducer
class SecondJobFirReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

    private final LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(LongWritable l : values){
            count+=l.get();
        }
        v.set(count);
        context.write(key,v);
    }
}



//第二个Job - Mapper
class SecondJobSecMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    private final Text k = new Text();
    private final LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] word = value.toString().split("\t");
        k.set(word[0].split(DataSkewDriver.SPLIT_CHAR)[0]);
        v.set(Long.parseLong(word[1]));
        context.write(k, v);
    }
}

//第二个Job - Reducer
class SecondJobSecReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

    private final LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(LongWritable l : values){
            count+=l.get();
        }
        v.set(count);
        context.write(key,v);
    }
}
