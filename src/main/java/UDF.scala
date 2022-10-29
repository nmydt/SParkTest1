import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object UDF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate

    import spark.implicits._
    spark.read.textFile("data/DSL.txt")
      .map(_.split(","))
      .map(x => (x(0),x(1),x(2)))
      .toDF("id","name","age")
      .cache()
  }

}
