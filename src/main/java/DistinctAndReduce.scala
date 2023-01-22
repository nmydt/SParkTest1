import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DistinctAndReduce {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))
    //distinct源码    reduceByKey((x,y)这里的x,y都是<key,value>中的value 返回<key,func(value)>
    val distinct1: RDD[Int] = rdd.map(x => (x, null)).reduceByKey((x, y) => x, 1).map(_._1)
    val distinct2: RDD[Int] = rdd.distinct(2)
    //查看distinct2分区
    //(0,6)
    //(1,1)
    //(0,2)
    //(1,9)
    //(1,5)
    distinct2.mapPartitionsWithIndex((index, items) => {
      items.map(x => (index, x))
    }).foreach(println)
    //查看distinct1分区
    //(0,1)
    //(0,6)
    //(0,9)
    //(0,5)
    //(0,2)
    distinct1.mapPartitionsWithIndex((index, items) => {
      items.map(x => (index, x))
    }).foreach(println)

    //reduce和reduceByKey区别
    //reduce : 1+2=3
    //3+3=6
    //6+4=10
    val c = sc.parallelize(1 to 10)
    c.reduce((x, y) => x + y) //结果55

    //reduceByKey : reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行binary_function的reduce操作，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
    val a = sc.parallelize(List((1, 2), (1, 3), (3, 4), (3, 6)))
    a.reduceByKey((x, y) => x + y).collect
    //结果 Array((1,5), (3,10))

    //continue与break
 
breakable(
    for(i<-0 until 10) {
      println(i)
      if(i==5){
        break()
      }
    }
  )
 // 0,1,2,3,4,5
(2)continue例子

for(i<-0 until 10){
      breakable{
      if(i==3||i==6) {
        break
      }
      println(i)
      }
    }
    //0,1,2,3,5,7,8,9
需要导入的包：


    sc.stop()
    spark.stop()
  }

}
