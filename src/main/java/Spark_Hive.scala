import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_Hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .enableHiveSupport()
      .getOrCreate

    import spark.implicits._
    // 开启Hive动态分区
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // 使用Spark DataFrame API创建Hive分区表，并把DataFrame的数据入库

    val df: DataFrame = spark.read.textFile("data/turple.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("id", "language")
    df.show()
    df.write.partitionBy("id").format("hive").saveAsTable("hive_part_tbl")

    spark.stop()
  }

}
