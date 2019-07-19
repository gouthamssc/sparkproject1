

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
def main(args: Array[String]): Unit = {
  
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

  
    
    val s = List("1","2","3")
    val d  = sc.parallelize(s)
    
    d.collect().foreach(println)
    
  }
}
