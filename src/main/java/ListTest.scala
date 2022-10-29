import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ListTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.parallelize(List("1", "2", "3"), 2)
    //不使用List和Array是因为没有append函数 , list存储的是rdd的每个分区的数据
    rdd.foreachPartition((part: Iterator[String]) => {
      val list = ArrayBuffer("")
      part.foreach((x: String) => {
        list.append(x)
      })
      println("111: " + list)
    })
    //all存储的是所有分区的数据 collect:可以将RDD类型的数据转化为数组，同时会从远程集群是拉取数据到driver端
    val all: Array[String] = rdd.collect()
    println(all.mkString(","))

    sc.stop()
    spark.stop()
  }

}
