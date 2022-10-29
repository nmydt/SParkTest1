import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object DSL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .master(master = "local[*]")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    import spark.sql
    val df = spark.read.textFile(path = "data/DSL.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2)))
      .toDF("id", "name", "age")
    df.cache()
    df.createTempView(viewName = "view")
    // 剔除年龄等于25以及名字为张三的用户
    val start1 = System.currentTimeMillis()
    sql(sqlText = "select * from view where age = 25 or name = '张三'")
      .createTempView(viewName = "view1")
    sql(sqlText = "select * from view where id not in (select id from view1)").show()
    val end1 = System.currentTimeMillis()
    println("方法一耗时：" + (end1 - start1))

    val start2 = System.currentTimeMillis()
    sql(sqlText = "select * from view")
      .where("age != 25")
      .where("name != '张三'")
      .show()
    val end2 = System.currentTimeMillis()
    println("方法二耗时：" + (end2 - start2))

    //窗口函数DSL在Windows.Scala中

    //性能测试
    val dfA = spark.read.textFile(path = "data/infoA.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("tel_number", "name")

    val dfB = spark.read.textFile(path = "data/infoB.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1)))
      .toDF("tel_number", "name")
    dfA.cache()
    dfB.cache()
    dfA.createTempView(viewName = "viewA")
    dfB.createTempView(viewName = "viewB")

    //方法一
    val start_time1 = System.currentTimeMillis()

    dfA.filter("tel_number not in (select tel_number from viewB)")
      .agg(count($"tel_number")).show()
    val end_time1 = System.currentTimeMillis()
    println("方法一耗时：" + (end_time1 - start_time1))

    //方法二
    val start_time2 = System.currentTimeMillis()
    dfA.join(dfB, usingColumns = Seq("tel_number"), joinType = "left_outer")
      .where(dfB("name").isNull)
      .agg(count($"tel_number")).show()
    val end_time2 = System.currentTimeMillis()
    println("方法二耗时：" + (end_time2 - start_time2))

    //方法三
    val start_time3 = System.currentTimeMillis()

    val telSet: mutable.HashSet[String] = mutable.HashSet() ++ dfB.select("tel_number")
      .map(row => row.mkString).collect()

    val telBroadcast = spark.sparkContext.broadcast(telSet)
    dfA.select("tel_number").map(row => row.mkString)
      .filter(x => !telBroadcast.value.contains(x))
      .toDF("tel_number")
      .agg(count($"tel_number")).show()

    val end_time3 = System.currentTimeMillis()
    println("方法三耗时：" + (end_time3 - start_time3))

    //参考链接：https://blog.csdn.net/Android_xue/article/details/102815261
    spark.stop()

  }

}
