package sparkcore

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 20210418-20210425 作业一
 * 使用Spark Core API操作HBase数据：自己在HBase中创建一个emp表，使用RDD API的方式将数据存入到HBase表中，
 * 		然后再使用RDD API读取emp表中的数据
 * 	emp.empno emp.ename	emp.job	emp.mgr	emp.hiredate emp.sal emp.comm emp.deptno
 */
object HBaseRDD {

	def main(args: Array[String]): Unit = {

		val SparkConf = new SparkConf().setMaster("local[4]").setAppName("HBaseRDD")
		val sc = new SparkContext(SparkConf)

		val conf = HBaseConfiguration.create()
		conf.set(TableInputFormat.INPUT_TABLE, "stu")

		// 1.插入数据
		val dataRDD = sc.makeRDD(List(("1004", "wangwu", "15"), ("1005", "zhaoliu", "20")))
		val putRDD = dataRDD.map{
			case (rowKey, name, age) =>
				val put = new Put(Bytes.toBytes(rowKey))
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age))
				(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
		}

		val jobConf = new JobConf(conf)
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, "stu")

		putRDD.saveAsHadoopDataset(jobConf)

		// 2.读取数据
		val HBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[ImmutableBytesWritable], classOf[Result])

		HBaseRDD.foreach({
			case (rowKey, result) =>
				result.rawCells().foreach(cell => {
					println("Row=" + Bytes.toString(rowKey.get()) + " column=" +
							Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
							Bytes.toString(CellUtil.cloneQualifier(cell)) + " value=" +
							Bytes.toString(CellUtil.cloneValue(cell)))
				})
		})
		sc.stop()
	}
}
