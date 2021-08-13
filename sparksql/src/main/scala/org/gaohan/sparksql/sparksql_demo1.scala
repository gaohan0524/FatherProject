package org.gaohan.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparksql_demo1 {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("sparksql_demo1").setMaster("local[2]")

		val spark = SparkSession.builder().config(conf).getOrCreate()

		import spark.implicits._

		val rdd = spark.sparkContext.textFile("D:\\Repos\\FatherProject\\sparksql\\data\\test.txt")
				.map{x => (x.split(",")(0).trim.toInt, x.split(",")(1), x.split(",")(2).trim.toInt)}

		// 三者转换
		val rddToDf = rdd.toDF("id", "name", "age")
		val rddToDs = rdd.map{x => person(x._1, x._2, x._3)}.toDS()
		val dfToDs = rddToDf.as[person]
		val dfToRdd = rddToDf.rdd
		val dsToRdd = rddToDs.rdd
		val dsToDf = rddToDs.toDF()

		// SQL
		rddToDf.createOrReplaceTempView("person")
		spark.sql("select * from person")

		// DSL
		rddToDf.select("*")

		// UDF
		spark.udf.register("addName", (x:String) => "Name:" + x)
		spark.sql("select *, addName(name) from people")
	}
}

case class person(id:Int, name:String, age:Int)