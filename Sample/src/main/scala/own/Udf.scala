package own



import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,SparkSession}
import java.util.Properties
object Udf {
  
  val spark = SparkSession.builder().appName("Own").master("local[4]").getOrCreate()
  import spark.implicits._

  
  
  case class Emp(name:String,id:Integer, role:String)
  
  
   def main(args: Array[String]): Unit = {
  
    
    
    val ss = Seq(Emp("Goutham0",15147,"AVP"),
        Emp("Goutham1",15148,"AVP"),
        Emp("Goutham2",15149,"AVP"),
        Emp("Goutham3",15150,"AVP"),
        Emp("Goutham4",15151,"AVP"))
  
        
val df = spark.sparkContext.parallelize(ss, 3).toDF()

val save = df.write.format("json").save("/Users/gouthamrajanala/Desktop/dww.json")


 df.show()

val b = df.createOrReplaceTempView("hi")

val d  = spark.sql("select name from hi where id >15147")

d.show()
  }
  
  
  
  
}