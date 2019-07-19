package own

import org.apache.spark.sql.SparkSession


object one {
 def main(args: Array[String]): Unit = {
    
  
  val spark = SparkSession.builder().appName("BIM").master("local[4]").getOrCreate()
  
  
  val s = List(2,3,4,1)
  val rdd = spark.sparkContext.parallelize(s,1)
  rdd.collect().foreach(println)
  
}
}



