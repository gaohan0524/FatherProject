package sparkcore

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 20210418-20210425
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

		val HBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
			classOf[ImmutableBytesWritable], classOf[Result])

		HBaseRDD.foreach({
			case (rowKey, result) => {
				result.rawCells().foreach(cell => {
					println(Bytes.toString(rowKey.get()) + "=" +
					Bytes.toString(CellUtil.cloneValue(cell)))
				})
			}
		})


		sc.stop()
	}
}



