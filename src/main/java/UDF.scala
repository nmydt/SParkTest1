import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object UDF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    import spark.implicits._
    import spark.sql
    val df_user: DataFrame = spark.read.textFile("data/DSL.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("id", "name", "age")
      .cache()
    spark.udf.register("addPrefix", (field: String) => randomPrefixUDF(field))
    spark.udf.register("rmPrefix", (field: String) => removePrefixUDF(field))

    df_user.createTempView(viewName = "view")
    val df_prefix = sql(sqlText = "select addPrefix(name) as pre_name  from view")
    df_prefix.show()
    df_prefix.createTempView(viewName = "pre_view")
    sql(sqlText = "select rmPrefix(pre_name) as name from pre_view").show()
    spark.stop()
  }

  /**
   * 给DataFrame指定字段随机加"_"前缀
   *
   * @param field 字段名称
   * @return 加完前缀后的值
   */
  def randomPrefixUDF(field: String): String = {
    val random = new Random()
    val prefix = random.nextInt(10)
    prefix + "_" + field
  }

  /**
   * 去除DataFrame指定字段随机加的"_"前缀
   *
   * @param field 字段名称
   * @return 去除前缀后的值
   */
  def removePrefixUDF(field: String): String = {
    field.split("_")(1)
  }

}
