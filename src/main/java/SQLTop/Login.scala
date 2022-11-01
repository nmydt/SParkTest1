package SQLTop

import org.apache.spark.sql.SparkSession

/**
 * Description: 求连续在线id
 * 第一列用户id、第二列用户登录时间
 */
object Login {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    import spark.implicits._
    spark.read.textFile("data/login_data.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("id", "dt")
      .cache()
      .createTempView("view1")

    var sqltext =
      """
        |WITH t1 AS (
        |	SELECT
        |		id,
        |		dt,
        |		lead (dt, 1) over (PARTITION BY id ORDER BY dt) AS ld
        |	FROM
        |		view1
        |) SELECT
        |	id
        |FROM
        |	t1
        |WHERE
        |	datediff(ld, dt) = 1
        |""".stripMargin


    spark.sql(sqltext).show()
    spark.stop()
  }
}
