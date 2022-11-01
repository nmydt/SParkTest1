package SQLTop

import org.apache.spark.sql.SparkSession

/**
 * Description: 活跃用户留存率(不是注册用户留存率)
 * 第一列用户id、第二列用户日志时间
 */
object Retain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    spark.read.csv("data/retain_data.txt")
      .toDF("id", "log_date")
      .cache()
      .createTempView("view1")

    spark.sql(
      """
        |WITH t1 AS (
        |	SELECT
        |		id,
        |		to_date (log_date) AS start_date
        |	FROM
        |		view1
        |	GROUP BY
        |		id,
        |		to_date (log_date)
        |),
        | t2 AS (
        |	SELECT
        |		id,
        |		to_date (log_date) AS end_date
        |	FROM
        |		view1
        |	GROUP BY
        |		id,
        |		to_date (log_date)
        |),
        | t3 AS (
        |	SELECT
        |		t1.id,
        |		t1.start_date,
        |		t2.end_date,
        |  datediff(end_date, start_date) AS diff_days
        |	FROM
        |		t1
        | JOIN t2 ON t1.id = t2.id
        |	WHERE
        |		t1.start_date <= t2.end_date
        |),
        | t4 AS (
        |	SELECT
        |		start_date,
        |		count(DISTINCT id) AS r0,
        |		count(
        |			DISTINCT
        |			IF (diff_days = 1, id, NULL)
        |		) AS r1,
        |		count(
        |			DISTINCT
        |			IF (diff_days = 3, id, NULL)
        |		) AS r3,
        |		count(
        |			DISTINCT
        |			IF (diff_days = 5, id, NULL)
        |		) AS r5
        |	FROM
        |		t3
        |	GROUP BY
        |		start_date
        |) SELECT
        |	start_date,
        |	r0,
        |	r1,
        |	r3,
        |	r5,
        |	concat(round(r1 / r0 * 100, 2), '%') AS r1_ratio,
        |	concat(round(r3 / r0 * 100, 2), '%') AS r3_ratio,
        |	concat(round(r5 / r0 * 100, 2), '%') AS r5_ratio
        |FROM
        |	t4
        | order by start_date
      """.stripMargin).show()
    spark.stop()

  }

}
