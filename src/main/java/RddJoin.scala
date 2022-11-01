import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object RddJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(this.getClass().getSimpleName())
      .master("local[*]")
      .config("", "")
      .getOrCreate
    val sc: SparkContext = spark.sparkContext
    //建立一个基本的键值对RDD，包含ID和名称，其中ID为1、2、3、4
    val rdd1 = sc.makeRDD(Array(("1", "Spark"), ("2", "Hadoop"), ("3", "Scala"), ("4", "Java"), ("5", "Python"), ("6", "C++")), 2)
    //建立一个行业薪水的键值对RDD，包含ID和薪水，其中ID为1、2、3、5
    val rdd2 = sc.makeRDD(Array(("1", "30K"), ("2", "15K"), ("3", "25K"), ("5", "10K")), 2)

    println("//下面做Join操作，预期要得到（1,×）、（2,×）、（3,×）")
    val joinRDD = rdd1.join(rdd2).collect.foreach(println)
    //(2,(Hadoop,15K))
    //(5,(Python,10K))
    //(3,(Scala,25K))
    //(1,(Spark,30K))

    println("//下面做leftOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(4,×）")
    val leftJoinRDD = rdd1.leftOuterJoin(rdd2).collect.foreach(println)
    //(4,(Java,None))
    //(6,(C++,None))
    //(2,(Hadoop,Some(15K)))
    //(5,(Python,Some(10K)))
    //(3,(Scala,Some(25K)))
    //(1,(Spark,Some(30K)))

    println("//下面做rightOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(5,×）")
    val rightJoinRDD = rdd1.rightOuterJoin(rdd2).collect.foreach(println)
    //(2,(Some(Hadoop),15K))
    //(5,(Some(Python),10K))
    //(3,(Some(Scala),25K))
    //(1,(Some(Spark),30K))

    sc.stop()
    spark.stop()
  }

}
