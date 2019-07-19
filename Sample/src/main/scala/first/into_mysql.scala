package first

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.log4j.{Level, Logger}

object into_mysql  extends App {

  val  spark = SparkSession.builder()
    .master("local")
    .appName("SQLCNCTN" )
    .getOrCreate()

val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.ERROR)
  val url = "jdbc:mysql://localhost:3306/goutham"

  val table = "miracle2"
 // val table1 ="mad4"
val table2 = "miracle3"

  val properties = new Properties()
  properties.put("user","root")
  properties.put("password","Gouthi@123")
  Class.forName("com.mysql.jdbc.Driver")


//val ncc = spark.read.jdbc(url,table1,properties)

val csv = spark.read.option("header","true").csv("/Users/gouthamrajanala/Downloads/full.csv")
val csv1 = spark.read.option("header", "true").csv("/Users/gouthamrajanala/Documents/sales.csv")
 //val updateDF = spark.sql("select table.* from table join table1 on table.id = table1.id")



//val tab = "goutham.ncjson"
 //csv.write.mode(SaveMode.Overwrite).jdbc(url,table,properties)
  csv.write.mode(SaveMode.Overwrite).jdbc(url,table2,properties)

 /*
 var dbTable =
        "(select count(*) from rew2) as r";


 
 
 val scc = spark.read.jdbc(url,dbTable,properties)
//val selApp = scc.select("*")
 //selApp.show()

*/
  
 
  
  //scc.show()
}
