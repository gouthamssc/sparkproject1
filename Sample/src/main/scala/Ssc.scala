

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,SparkSession,SaveMode}
object Ssc {
  
  
  def main(args: Array[String]){


    val spark = SparkSession.builder().appName("RDD").master("local[4]").getOrCreate()
    
    //variables
    var i: Int = 1
    var i2: Short = 2
    var i3: Long = 23L
    var i4: Float = 2.33f
    var i5: Boolean = true
    var i6: Char = 'A'

    //collections


    var c1: Array[Int] = Array(1, 2, 3, 4, 5)
    val c11 = spark.sparkContext.parallelize(c1,1)
    var c2: Array[String] = Array("this", "is", "scala", "program")
    val c12 = spark.sparkContext.parallelize(c2,1)

    var l1: List[Int] = List(1, 2, 3, 4, 5)
    val l11 = spark.sparkContext.parallelize(l1,1)

    var l2: List[String] = List("yeah", "i", "am", "writing scala")
    val l12 = spark.sparkContext.parallelize(l2,1)

    var s1: Seq[Int] = Seq(9, 8, 7, 6, 5, 4)
    var s2: Seq[String] = Seq("bindass", "kirackk", "patas")
    val s11 = spark.sparkContext.parallelize(s1,1)
    val s12 = spark.sparkContext.parallelize(s2,1)


    var m1: Map[Int, String] = Map((1, "goutham"), (2, "ssc"))


    var r1:Range = Range(1, 11)
val r11 = spark.sparkContext.parallelize(r1, 1)
    
    println("The variables declared are \n" + i + "\n" + i2 + "\n" + i3 + "\n" + i4 + "\n" + i5 + "\n" + i6+"\n")

    println("The collections declared are \n" + c1.mkString + "\n" + c2.mkString + "\n" + l1 + "\n" + l2 + "\n" + s1 + "\n" + s2 +"\n"+m1(1)+"\n"+r1)
    
    
    println("The rddss declared are \n" + c11.collect().mkString + "\n" + c12.collect().mkString  + "\n" + l11.collect().mkString  + "\n" + l12.collect().mkString  + "\n" + s11.collect().mkString  + "\n" + s12.collect().mkString +"\n"+m1(1)+"\n"+r11.collect().mkString 
        )

    
  }
  
  
  
  
  
}