package sparkcore

import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 20210418-20210425 作业二
 * 准备1000w条数据，表中准备2个cf，分别是o1和o2，o1的字段有id name age，o2的字段有sex、address。
 * 提供你的数据插入到HBase表中一共花费了多少时间。
 * 参考值：30-60秒内单机测试完成
 */
object SparkHFile {

	def main(args: Array[String]): Unit = {
		val start_time =new Date().getTime

		val inputFile = args(0)
		//val inputFile = "D:\\Repos\\phaseTwo\\mock\\out\\hbasedata.dat"
		val output = "hdfs://192.168.44.12:8020/hfile"

		System.setProperty("HADOOP_USER_NAME", "root")
		val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkHFile")
		val sc = new SparkContext(sparkConf)
		val hadoopConf = new Configuration()
		val tableName = "insert"
		hadoopConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

		// 将文件导出至HFile
		val cfs = Array("o1".getBytes, "o2".getBytes)
		val cols = Array("id".getBytes, "name".getBytes, "age".getBytes, "sex".getBytes, "address".getBytes)
		sc.textFile(inputFile)
				.map(x => {
					val splits = x.split(",")
					val rowKey = Bytes.toBytes(splits(0).toInt)
					Array(
						(new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, cfs(0), cols(2), splits(2).getBytes()))
						, (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, cfs(0), cols(0), splits(0).getBytes()))
						, (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, cfs(0), cols(1), splits(1).getBytes()))
						, (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, cfs(1), cols(4), splits(4).getBytes()))
						, (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, cfs(1), cols(3), splits(3).getBytes()))
					)
				}).flatMap(x => x)
				.saveAsNewAPIHadoopFile(output,
					classOf[ImmutableBytesWritable],
					classOf[KeyValue],
					classOf[HFileOutputFormat2],
					hadoopConf)

		val end_time1 =new Date().getTime
		println(s"HFile generated! time costs：${(end_time1-start_time) / 1000.0}s")


		// 将HFile导入至HBase
		val hbaseConf = HBaseConfiguration.create()
		val load = new LoadIncrementalHFiles(hbaseConf)
		val conn = ConnectionFactory.createConnection(hbaseConf)
		val table: Table = conn.getTable(TableName.valueOf(tableName))

		try {
			val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
			val job = Job.getInstance(hbaseConf)
			job.setJobName("LoadFile")
			// 此处最重要，需要设置文件输出的key，要生成HFile，所以OutputKey要用ImmutableBytesWritable
			job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
			job.setMapOutputValueClass(classOf[KeyValue])

			// 配置HFileOutputFormat2的信息
			HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

			// bulkload后会删除hfile，要做好备份
			load.doBulkLoad(new Path(output), table.asInstanceOf[HTable])

		} finally {
			table.close()
			conn.close()
		}

		println("data load completed！")
		val end_time2 =new Date().getTime
		println(s"Total costs: ${(end_time2-start_time) / 1000.0}s")

		sc.stop()
	}
}
