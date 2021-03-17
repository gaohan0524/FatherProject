package org.gaohan.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * master 和appname是写死的，硬编码
 * 这2个参数应该是在spark-submit时来指定
 **/
object SparkWCApp {
	def main(args: Array[String]): Unit = {
		System.setProperty("HADOOP_USER_NAME", "root")
		val sparkConf: SparkConf = new SparkConf().setAppName("SparkWCApp").setMaster("local[4]")
		val sc = new SparkContext(sparkConf)

	// 将集合元素转成RDD，然后再进行后续的处理
	//    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6),3)
	//    rdd.foreach(println)

	//    sc.makeRDD(List(1, 2, 3, 4, 5, 6))

	//    Thread.sleep(Int.MaxValue)

	// HadoopRDD.map(pair => pair._2.toString)
	//    val rdd = sc.textFile("data/wc.data")
	//    rdd.foreach(println)


	// RDD 两大操作：transformation  action
	//    sc.hadoopFile("data/wc.data",
	//      classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
	//      1).map(x => {
	//      val tmp = x._2.toString
	//      println(tmp)
	//    }).foreach(println)

		// 使用hdfs上的数据
//		sc.hadoopConfiguration.set("dfs.replication", "1")
//		sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.44.12:8020")
//
//		sc.textFile("hdfs://192.168.44.12:8020/usr/data/wordcount.txt").foreach(println)

//		// 对比map mapPartition mapPartitionWithIndex
//		// Return a new RDD by applying a function to all elements of this RDD.
//		sc.makeRDD(List(1,2,3,4)).map(x => x*2).collect()
//		// Return a new RDD by applying a function to each partition of this RDD.
//		sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10), 3)
//				.mapPartitions(partition => {
//					println("---这是一个分区---")
//					partition.map(_ * 2)
//				}).foreach(println)
//		// Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.
//		sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),2)
//				.mapPartitionsWithIndex((index, partition) => {
//					println("---这是一个分区---")
//					partition.map(x => {
//						s"分区:$index, 元素:$x"
//					})
//				}).foreach(println)

		val rdd = sc.parallelize(List(List(1, 2, 3), List(4, 5, 6)))
		rdd.map(x => x.map(_ * 2)).foreach(println)
		rdd.flatMap(x => x.map(_ * 2)).foreach(println)

		val rdd1 = sc.parallelize(List(1,2,3,4))
		// 链式
		rdd1.filter(_ % 2 == 0).filter(_ > 2).foreach(println)
		rdd1.filter(x => x % 2 == 0 && x > 2).foreach(println)

		// Return an RDD created by coalescing all elements within each partition into an array.
		sc.parallelize(1 to 10, 3).glom().collect()

		// Return a sampled subset of this RDD.
		sc.parallelize(1 to 20).sample(withReplacement = false, 0.2).collect()

		val a = sc.makeRDD(List("若泽", "PK", "xingxing"))
		val b = sc.makeRDD(List(30, 31, 18))
		a.zip(b).zipWithIndex().foreach(println)
		a.zip(b).foreach(println)

		sc.stop()
	}

}
