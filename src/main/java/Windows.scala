import org.apache.spark.sql.SparkSession

object Windows {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    import spark.implicits._
    spark.read.textFile("data/windows.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("company", "app", "vst_times")
      .cache()
      .createTempView("a1")
    //需求一：取出BAT三大公司访问量Top2的app

    spark.sql("select * from a1").show()
    var sqltext =
      """SELECT company, app, vst_times
        |FROM (
        |	SELECT company, app, vst_times, row_number() OVER (PARTITION BY company ORDER BY vst_times DESC) AS rn
        |	FROM a1
        |) temp
        |WHERE rn <= 2""".stripMargin
    spark.sql(sqltext).show()
    //DSL形式：
    //val df = spark.read.textFile("data/windows.txt")
    //      .map(_.split(","))
    //      .map(x => (x(0), x(1), x(2)))
    //      .toDF("company", "app", "vst_times")
    //      .cache()

    //val windows = Window.partitionBy("company").orderBy(col("vst_times").desc)
    //    //取出BAT三大公司访问量Top2的app
    //    df.select("company", "app", "vst_times")
    //      .withColumn("row_number", row_number().over(windows))
    //      .where("row_number <= 2 ")
    //      .select("company", "app", "vst_times")
    //      .show()

    //需求二：统计出腾讯系总访问次数前三的app
    var sqltext2 =
      """SELECT app
        |FROM a1
        |WHERE company = "腾讯"
        |ORDER BY vst_times DESC
        |LIMIT 3""".stripMargin
    spark.sql(sqltext2).show()

    //DSL形式：
    //    df.select("company", "app", "vst_times")
    //      .groupBy("company", "app")
    //      .agg(sum($"vst_times").alias("s_vst"))
    //      .sort($"s_vst".desc)
    //      .limit(3)
    //      .show()

    //UDTF函数 explode

    //    var sqltext2 =
    //      """SELECT company, app, vst_times, col
    //        |FROM a1
    //        |	LATERAL VIEW explode(split(company, ',')) temp1 AS col
    //      """.stripMargin
    //    spark.sql(sqltext2).show()

    spark.stop()
  }

}
