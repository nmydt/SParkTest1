import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
 * Description: 累加器只能在Driver端读取，在Excutor端更新
 */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    val sc: SparkContext = spark.sparkContext
    val accumulator: LongAccumulator = sc.longAccumulator
    var num = 0;
    sc.textFile("data/bd.txt")
      .map(x => {
        accumulator.add(1)
        num += 1
      }).collect()
    println("num: " + num)
    println("accumulator: " + accumulator.value)

    sc.stop()
    spark.stop()
  }

}
