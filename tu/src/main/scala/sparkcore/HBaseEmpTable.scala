package sparkcore

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  20210418-20210425 作业三
 *  HBase和Hive关联操作【以emp表为例】
 *  案例二:
 *  在HBase表中的数据，可以使用Hive SQL完成统计分析(deptno分组求每个组的人数)
 */
object HBaseEmpTable {
	def main(args: Array[String]): Unit = {

		val SparkConf = new SparkConf().setMaster("local[4]").setAppName("HBaseRDD")
		val sc = new SparkContext(SparkConf)

		val conf = HBaseConfiguration.create()

		// 插入数据
		// emp.empno emp.ename	emp.job	emp.mgr	emp.hiredate emp.sal emp.comm emp.deptno
		val dataRDD = sc.textFile("tu/input/emp.txt").map(x => x.split(" "))
		val putRDD = dataRDD.map{ x => {
			val put = new Put(Bytes.toBytes(x(0)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ename"), Bytes.toBytes(x(1)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("job"), Bytes.toBytes(x(2)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mgr"), Bytes.toBytes(x(3)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("hiredate"), Bytes.toBytes(x(4)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sal"), Bytes.toBytes(x(5)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("comm"), Bytes.toBytes(x(6)))
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("deptno"), Bytes.toBytes(x(7)))

			(new ImmutableBytesWritable(Bytes.toBytes(x(0))), put)
			}
		}

		val jobConf = new JobConf(conf)
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, "emp")

		putRDD.saveAsHadoopDataset(jobConf)

		sc.stop()
	}
}
