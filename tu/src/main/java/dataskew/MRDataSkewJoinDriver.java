package dataskew;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

/**
 * 20210328-20210410 作业三
 * 使用MapReduce解决 大表join大表 导致的数据倾斜
 *
 * 解决方案：reduceJoin + BloomFilter
 * 目的：通过两张大表join 操作，实现 一张大张 是否存在另一个大表中 ====>  inner join 操作
 */
public class MRDataSkewJoinDriver {

    public static void main(String[] args) throws Exception {
        //1. 先处理 布隆过滤器 - 生成二进制文件数据
        TrainingBloomfilter.createBloomFilterFile();

        String input = "tu/input/ruoze000_001.txt";
        String output = "tu/output";

        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//                .getRemainingArgs();
//        System.out.println("================ " + otherArgs[0]);
//        if (otherArgs.length != 3) {
//            System.err.println("Usage: BloomFiltering <in> <out>");
//            System.exit(1);
//        }

        FileSystem.get(conf).delete(new Path(output), true);

        Job job = new Job(conf, "TestBloomFiltering");
        job.setJarByClass(MRDataSkewJoinDriver.class);
        // 设置Mapper.class
        job.setMapperClass(BloomFilteringMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Access.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        DistributedCache.addCacheFile(new Path(TrainingBloomfilter.BLOOM_FILTER_FILE_PATH).toUri(),
////                job.getConfiguration());

        job.addCacheFile(new Path(TrainingBloomfilter.BLOOM_FILTER_FILE_PATH).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class BloomFilteringMapper extends Mapper<Object, Text, Access, NullWritable> {

        private final BloomFilter filter = new BloomFilter();

        /**
         * 通过缓存文件，获取 二进制数据 ，并封装 、添加到 布隆过滤器
         */
        @Override
        protected void setup(Context context) {
            BufferedReader in = null;
            try {
                // 从当前作业中获取要缓存的文件
//                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                URI[] cacheFiles = context.getCacheFiles();
                for (URI path : cacheFiles) {
                    if (path.toString().contains("bloom.bin")) {
                        DataInputStream strm = new DataInputStream(new FileInputStream(path.toString()));
                        // Read into our Bloom filter.
                        filter.readFields(strm);
                        strm.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Get the value for the comment
            String comment = value.toString();
            // If it is null, skip this record
            if (comment == null || comment.isEmpty()) {
                return;
            }
            String[] line = comment.split("\t");
            String ip = line[1];
            String trafficStr = line[3];
            //脏数据处理
            long traffic = StringUtils.isNoneBlank(trafficStr)
                    ? Long.parseLong(trafficStr.replace("ruoze","")) : 0L;

            //根据IP 匹配 出：源数据 Access ， 并输出
            StringTokenizer tokenizer = new StringTokenizer(ip);
            // For each word in the comment
            while (tokenizer.hasMoreTokens()) {
                // Clean up the words
                String cleanWord = tokenizer.nextToken();
                // If the word is in the filter, output it and break
                if (cleanWord.length() > 0
                        && filter.membershipTest(new Key(cleanWord.getBytes()))) {
                    Access access = new Access(line[0], ip, line[2], traffic);
                    context.write(access, NullWritable.get());
                    // break;
                }
            }
        }
    }

}

/**
 * 步骤1： 生成 布隆过滤器 源文件(分布式缓存文件)  -  生成二进制文件数据
 *      文件输出： D:\tmp\BloomFiler\bloom.bin
 */
class TrainingBloomfilter {

    public static final String BLOOM_FILTER_FILE_PATH = "D:\\tmp\\BloomFiler\\bloom.bin";

    /**
     *
     * @param numRecords
     * @param falsePosRate
     * @return
     */
    public static int getOptimalBloomFilterSize(int numRecords,
                                                float falsePosRate) {
        int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
                .pow(Math.log(2), 2));
        return size;
    }

    /**
     *
     *   BloomFilter参数计算方式：
     *           n：小表中的记录数。
     *           m：位数组大小，一般m是n的倍数，倍数越大误判率就越小，但是也有内存限制，不能太大，这个值需要反复测试得出。
     *           k：hash个数，最优hash个数值为：k = ln2 * (m/n)
     *
     * @param numMembers
     * @param vectorSize
     * @return
     */
    public static int getOptimalK(float numMembers, float vectorSize) {
        return (int) Math.round(vectorSize / numMembers * Math.log(2));
    }



    public static void createBloomFilterFile() throws IOException {
        String input = "tu/input/ruozeIp.txt";
        Path inputFile = new Path(input);
        int numMembers = Integer.parseInt("10");
        float falsePosRate = Float.parseFloat("0.01");
        Path bfFile = new Path(BLOOM_FILTER_FILE_PATH);

        // Calculate our vector size and optimal K value based on approximations
        int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
        int nbHash = getOptimalK(numMembers, vectorSize);

        // create new Bloom filter
        BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

        // Open file for read
        System.out.println("Training Bloom filter of size " + vectorSize
                + " with " + nbHash + " hash functions, " + numMembers
                + " approximate number of records, and " + falsePosRate
                + " false positive rate");

        String line = null;
        int numRecords = 0;
        FileSystem fs = FileSystem.get(new Configuration());
        for (FileStatus status : fs.listStatus(inputFile)) {
            BufferedReader rdr;
            // if file is gzipped, wrap it in a GZIPInputStream
            if (status.getPath().getName().endsWith(".gz")) {
                rdr = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(fs.open(status.getPath()))));
            } else {
                rdr = new BufferedReader(new InputStreamReader(fs.open(status
                        .getPath())));
            }

            System.out.println("Reading " + status.getPath());
            while ((line = rdr.readLine()) != null) {
                filter.add(new Key(line.getBytes()));
                ++numRecords;
            }
            rdr.close();
        }

        System.out.println("Trained Bloom filter with " + numRecords + " entries.");

        System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
        FSDataOutputStream strm = fs.create(bfFile);
        filter.write(strm);

        strm.flush();
        strm.close();

        System.out.println("Done training Bloom filter.");
    }

}

