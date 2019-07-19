package first

import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.google.common.io.Resources
import scala.io.Source

import java.sql.DriverManager
import org.apache.spark.sql.SQLContext

object mysql_connection extends App {


   val  spark = SparkSession.builder().master("local[*]").appName("SQLCNCTN" ).config("spark.executor.memory", "16g").config("spark.driver.memory", "16g").getOrCreate()
   


val url1 = "jdbc:db2://9.212.133.44:60102/cmdw_tpt"
//  val table1 = "WW.BIM_CONFIG_CYCLE"

   val url = "jdbc:db2://9.212.130.127:38362/CE05CDA1"
  //val table = "DBDFMSDR"
  //val table =  "WW.FMST_O_ITT_CUSTOMER_LVL_TARGET"

  //val table = spark.sparkContext.parallelize(tab,53)
  // val table = "(select * from WW.FMST_O_ITT_CUSTOMER_LVL_TARGET) as c"
    //rdd = "(select count(*) from WW.FMST_O_ITT_CUSTOMER_LVL_TARGET) as c"
    
 

//println(url)

   val properties = new Properties()
    properties.put("user", "iavp3ag")
    properties.put("password", "gout1234")
     properties.put("sslConnection", "true");
     properties.put("sslTrustStoreLocation","/Users/gouthamrajanala/Documents/mopcloud-tls-truststore.jks")
 // properties.put("KeystoreLocation","/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/security/cacerts")
//   properties.put("sslTrustStorePassword","XXXXXX")
 // properties.put("keystore.password","changeit")
    Class.forName("com.ibm.db2.jcc.DB2Driver")

//Hyd address 
//16-2-836/5 sri satya ram sai apts,lic colony,saidabad

   // val conn = spark.sqlContext.read.jdbc(url,"FMST_O_ITT_CUSTOMER_LVL_TARGET,FMST_O_ITT_CUSTOMER_FIN_T", properties)

    
    //conn.show()
    
    val custTable = spark.read.jdbc(url,table,properties)
  val s = custTable.rdd.getNumPartitions
  println(s);  

 //val sqlQuery = Source.fromFile("/Users/gouthamrajanala/Documents/new.sql").getLines.mkString 

 // val stcust = custTable.select("Name").where("Department = 'POLICE' ")

//  stcust.show()
//val d = custTable.createOrReplaceTempView("Hi")
//val s1 = spark.sql("SELECT * FROM Hi")
//s1.show()
 //s.write.csv("/Users/gouthamrajanala/Desktop/cm.csv")
}  


