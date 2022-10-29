import org.apache.spark.sql.SparkSession

object DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    import spark.implicits._
    spark.read.textFile("data/clm.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("id", "name", "age")
      .cache().createTempView("clm")
    spark.read.textFile("data/clm2.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("id", "place")
      .cache()
      .createTempView("clm2")
    // not in 与左连接  查询数据表1 并且数据表1的数据不在数据表2中
    var sqltext = "select id,name,age from clm where id not in (select id from clm2)"
    spark.sql(sqltext).show()
    val sqltext2 = "select clm.id,name,age from clm left join clm2 on clm.id=clm2.id where clm2.id is Null"
    spark.sql(sqltext2).show()

    // 1, 测试默认数据类型
    spark.read.
      textFile("data/clm.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("id", "name", "age")
      .dtypes
      .foreach(println)

    //2, 把数值型的列转为IntegerType
    spark.read.
      textFile("data/clm.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("id", "name", "age")
      .select($"id".cast("int"), $"name", $"age".cast("int"))
      .dtypes
      .foreach(println)

    spark.stop()
  }

}
