package dataskew;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 20210328-20210410 作业三
 * 使用MapReduce解决 group by 导致的数据倾斜
 */
public class MRDataSkewGroupByDriver {

    public static void main(String[] args) throws Exception{
        String input = "tu/input/ruoze000_001.txt";
        String output = "tu/output";

        Configuration configuration = new Configuration();
        // 1.获取Job对象
        Job job = Job.getInstance(configuration);
        // 删除输出文件
        FileUtil.deleteTargetedFile(configuration, output);
        // 2、设置class
        job.setJarByClass(MRDataSkewGroupByDriver.class);
        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        // Combiner  -- 设置maptask端的局部聚合逻辑类 - 减少io - 减少reducer压力 [数据倾斜优化]
        job.setCombinerClass(MyReducer.class);

        // 自定义分区 -- 将倾斜的key - 进行打散 [数据倾斜优化]
        job.setPartitionerClass(AccessPartitioner.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Access> {

        Logger logger = LoggerFactory.getLogger(MyMapper.class);

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] line = value.toString().split("\t");
            try {
                String ip = line[1];
                String trafficStr = line[3];
                long traffic = StringUtils.isNoneBlank(trafficStr) ? Long.parseLong(trafficStr) : 0L;
                Access access = new Access(line[0], ip, line[2], traffic);

                context.write(new Text(ip), access);
            }catch (Exception e){
//                e.printStackTrace();
                logger.info("ERR-Data - time:{} ",line[0]);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Access, Text, Access> {
        @Override
        protected void reduce(Text ip, Iterable<Access> values, Context context) throws IOException, InterruptedException {
            long traffic = 0;

            for (Access access : values) {
                traffic += access.getTraffic();
            }

            Access access = new Access("",ip.toString(),"",traffic);

            context.write(ip, access);
        }
    }

    /**
     * 自定义分区
     *   remark: 处理 key - 数据倾斜 ，将倾斜的key - 进行打散
     */
    public static class AccessPartitioner extends Partitioner<Text,Access> {

        @Override
        public int getPartition(Text text, Access access, int i) {
            String ip = text.toString();
            return (ip.hashCode() & Integer.MAX_VALUE) % i;
        }
    }
}

class Access implements Writable {

    private String time;
    private String ip;
    private String url;
    private long traffic;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(time);
        out.writeUTF(ip);
        out.writeUTF(url);
        out.writeLong(traffic);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.time = in.readUTF();
        this.ip = in.readUTF();
        this.url = in.readUTF();
        this.traffic = in.readLong();
    }

    @Override
    public String toString() {
        return "IP:" + this.ip + " Traffic:"+ this.traffic;
    }

    public Access() {}

    public Access(String time, String ip, String url, long traffic) {
        this.time = time;
        this.ip = ip;
        this.url = url;
        this.traffic = traffic;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTraffic() {
        return traffic;
    }

    public void setTraffic(long traffic) {
        this.traffic = traffic;
    }
}
