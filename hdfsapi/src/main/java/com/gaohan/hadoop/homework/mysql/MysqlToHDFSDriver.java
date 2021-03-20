package com.gaohan.hadoop.homework.mysql;

import com.gaohan.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR读取MySQL的作业打包到服务器运行： MySQL ==> HDFS
 */
public class MysqlToHDFSDriver {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        String outputPath = "hdfs://192.168.217.10:8020/output";

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("fs.defaultFS", "hdfs://192.168.217.10:8020");

        FileUtils.deleteTargetedFile(conf, outputPath);

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.217.10:3306/ruozedata?useUnicode=true&characterEncoding=utf-8&useSSL=false",
                "root",
                "123456Gh!");

        Job job = Job.getInstance(conf);

        // 运行前将jar包传到hdfs
        job.addFileToClassPath(new Path("hdfs://192.168.217.10:8020/ruozedata/mysql-connector-java-5.1.49.jar"));

        job.setJarByClass(MysqlToHDFSDriver.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MysqlReader.class);

        String[] fieldNames = {"id", "name", "salary"};
        DBInputFormat.setInput(job, MysqlReader.class, "emp", null, null, fieldNames);
        //DBInputFormat.setInput(job, MysqlReader.class, "select * from emp", null);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, MysqlReader, NullWritable, MysqlReader> {

        @Override
        protected void map(LongWritable key, MysqlReader value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }
}
