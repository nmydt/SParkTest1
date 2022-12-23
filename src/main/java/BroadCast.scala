import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Description: 广播变量只读不可变
 */
object BroadCast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    val sc: SparkContext = spark.sparkContext
    val list = List("spark")
    print(list.contains("spark"))
    var broadcast: Broadcast[List[String]] = sc.broadcast(list)
    sc.textFile("data/bd.txt").filter((line: String) => {
      broadcast.value.contains(line)
    }).foreach(println)
    
    //删除广播变量
    broadcast.unpersist()
    //重新更新广播变量
    broadcast = sc.broadcast(list)
    
    sc.stop()
    spark.stop()
  }

}
