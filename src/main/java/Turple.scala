import org.apache.spark.sql.SparkSession

object Turple {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate
    import spark.implicits._
    //Scala中Tuple的元素个数最多不能超过22个 使用数组处理
    //      spark.read.textFile("data/turple.txt")
    //        .map(_.split(","))
    //        .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10),
    //          x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22)))

    spark.read.textFile("data/turple.txt")
      .map(_.split(","))
      .select($"value"(0).cast("string").as("col0"),
        $"value"(1).cast("string").as("col1"),
        $"value"(2).cast("string").as("col2"),
        $"value"(3).cast("string").as("col3"),
        $"value"(4).cast("string").as("col4"),
        $"value"(5).cast("string").as("col5"),
        $"value"(6).cast("string").as("col6"),
        $"value"(7).cast("string").as("col7"),
        $"value"(8).cast("string").as("col8"),
        $"value"(9).cast("string").as("col9"),
        $"value"(10).cast("string").as("col10"),
        $"value"(11).cast("string").as("col11"),
        $"value"(12).cast("string").as("col2"),
        $"value"(13).cast("string").as("col13"),
        $"value"(14).cast("string").as("col14"),
        $"value"(15).cast("string").as("col15"),
        $"value"(16).cast("string").as("col16"),
        $"value"(17).cast("string").as("col17"),
        $"value"(18).cast("string").as("col18"),
        $"value"(19).cast("string").as("col19"),
        $"value"(20).cast("string").as("col20"),
        $"value"(21).cast("string").as("col21"),
        $"value"(22).cast("string").as("col22"),
        $"value"(23).cast("string").as("col23"),
        $"value"(24).cast("string").as("col24"))
      .show()

      spark.stop()
  }

}
