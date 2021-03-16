package com.gaohan.hadoop.homework.api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

/**
 * Scala操作HDFS API实现 mkdir put get list
 */
object HDFSByScalaApp {

	val configuration = new Configuration();
	configuration.set("dfs.replication", "1");
	val uri = new URI("hdfs://192.168.44.12:8020");
	val fileSystem: FileSystem = FileSystem.get(uri, configuration, "root");

	val path1: Path = new Path("/hdfsapi/scala")
	val localSrc: Path = new Path("data/wc.data")
	val remoteDst: Path = new Path("/hdfsapi/scala")
	val remoteSrc: Path = new Path("/hdfsapi/scala/wc.data")
	val localDst: Path = new Path("data")
	val listPath: Path = new Path("/hdfsapi")

	def mkdir(path: Path): Unit = {

		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true)
		}

		fileSystem.mkdirs(path)

	}

	def put(src: Path, dst: Path): Unit = {

		fileSystem.copyFromLocalFile(src, dst)
	}

	def get(src: Path, dst: Path): Unit = {

		fileSystem.copyToLocalFile(src, dst)
	}

	def list(listPath: Path): Unit = {
		val files = fileSystem.listFiles(listPath, true)
		while (files.hasNext) {
			val fileStatus = files.next()

			val isDir = fileStatus.isDirectory
			val permission = fileStatus.getPermission.toString
			val replication = fileStatus.getReplication
			val len = fileStatus.getLen
			val path = fileStatus.getPath.toString

			System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path)
		}
	}

	def main(args: Array[String]): Unit = {
		mkdir(path1)
		put(localSrc, remoteDst)
		get(remoteSrc, localDst)
		list(listPath)
		fileSystem.close()
	}

}
