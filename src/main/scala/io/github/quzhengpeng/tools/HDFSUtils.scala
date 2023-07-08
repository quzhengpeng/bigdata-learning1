package io.github.quzhengpeng.tools

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI

object HDFSUtils {

  val hadoopConf = new org.apache.hadoop.conf.Configuration()

  def readAsText(dir: String, sc: SparkContext): RDD[String] = {
    sc.textFile(dir)
  }

//  def readAsLzo(dir: String, sc: SparkContext): RDD[String] = {
//    sc.newAPIHadoopFile(dir,
//      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
//      classOf[org.apache.hadoop.io.LongWritable],
//      classOf[org.apache.hadoop.io.Text]).map(_._2.toString)
//  }

//  def readAsParquet(dir: String, sqlContext: SQLContext): DataFrame = {
//    sqlContext.read.format("parquet").load(dir)
//  }

  def readAsAvro() = {

  }

  def saveAsTextFile[T](data: RDD[T], path: String, host: String): Unit = {
    delPath(host, path)
    data.saveAsTextFile(path)
  }

//  def saveAsLzoFile[T](data: RDD[T], path: String, host: String): Unit = {
//    delPath(host, path)
//    data.saveAsTextFile(path, classOf[com.hadoop.compression.lzo.LzopCodec])
//  }

//  def saveAsParquetFile(data: DataFrame, path: String, host: String, sc: SparkContext): Unit = {
//    delPath(path, host)
//    data.write.parquet(path)
//  }

  def saveAsAvroFile() = {

  }

  def delPath(host: String, path: String): Boolean = {

    if (host.startsWith("hdfs"))
      delHDFSPath(host, path)
    else if (host.startsWith("local"))
      delLocalPath(path)
    else {
      print("The host is unknow.")
      //      throw IllegalArgumentException
      false
    }
  }

  def delLocalPath(path: String): Boolean = {

    val local_path = scala.reflect.io.Path(path)

    if (local_path.exists)
      local_path.deleteRecursively()
    else
      true
  }

  def delHDFSPath(host: String, path: String, isRecusrive: Boolean = true): Boolean = {

    val hdfs = FileSystem.get(new URI(host), hadoopConf)
    val hdfs_path = new Path(path)

    if (hdfs.exists(hdfs_path))
      hdfs.delete(hdfs_path, isRecusrive)
    else
      true
  }
}
