package SQLTop

import org.apache.spark.sql.SparkSession

/**
 * Description: 最大在线人数
 * 第一列用户id、第二列用户上线时间、第三列用户下线时间
 */
object Online {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()
    val df = spark.read.csv("data/online_data.txt").toDF("id", "start_time", "end_time")
    df.createOrReplaceTempView("view1")
    //暴力解法，效率较低
    //    spark.sql(
    //      """
    //        |	SELECT
    //        |   a.start_time,
    //        |   a.end_time,
    //        |		count(*) AS curr_online_cnt
    //        |	FROM
    //        |		view1 a,
    //        |		view1 b
    //        |	WHERE
    //        |		a.start_time BETWEEN b.start_time
    //        |	AND b.end_time
    //        |	GROUP BY
    //        |		a.start_time,
    //        |		a.end_time
    //      """.stripMargin).show()

    //利用开窗函数累加 上线一人加1，下线一人减1
    spark.sql(
      """
        |WITH t1 AS (
        |		SELECT id, start_time AS tt, 1 AS flag
        |		FROM view1
        |		UNION ALL
        |		SELECT id, end_time AS tt, -1 AS flag
        |   FROM view1
        |	),
        |	t2 AS (
        |		SELECT id, tt, sum(flag) OVER (ORDER BY tt) AS online
        |		FROM t1
        |	)
        |SELECT max(online)
        |FROM t2
      """.stripMargin).show()

  }

}
