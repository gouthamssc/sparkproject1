package first
import org.apache.spark.sql.SparkSession
import java.util.Properties
import scala.io.Source

object db2Connection {
  def  main(args: Array[String]): Unit = {
    
 
  val  spark = SparkSession.builder()
    .master("local[*]")
    .appName("DB2CNCTN" ).config("spark.executor.memory", "16g").config("spark.driver.memory", "16g").getOrCreate()
    

  val url = "jdbc:db2://9.212.130.127:38362/CE05CDA1"
  val table = "DBDFMSDR.FMST_O_ITT_COVREF"

 //val url1 = "jdbc:db2://9.212.133.44:60102/cmdw_tpt"
  //val table1 = "WW.FMST_O_ITT_CUSTOMER_LVL_TARGET"

   //val url2 = "jdbc:db2://9.212.133.44:60102/cmdw_tpt"
 // val table2 = "WW.FMST_O_ITT_CUST"
  
  val properties = new Properties()
    properties.put("user","IAVP3AG")
    properties.put("password","GOUT1234")
   properties.put("sslConnection", "true");
   properties.put("sslTrustStoreLocation","/Users/gouthamrajanala/Documents/mopcloud-tls-truststore.jks")
 // properties.put("KeystoreLocation","/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/security/cacerts")
//   properties.put("sslTrustStorePassword","XXXXXX")
 // properties.put("keystore.password","changeit")
    Class.forName("com.ibm.db2.jcc.DB2Driver")
/*
    val properties1 = new Properties()
    properties1.put("user","tpt_ww")
    properties1.put("password","tptwwdog")
   properties1.put("sslConnection", "true");
   properties1.put("sslTrustStoreLocation","/ZOS1CP/tmp/ibm-ssl-truststore.jks")
 // properties.put("KeystoreLocation","/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/security/cacerts")
//   properties.put("sslTrustStorePassword","XXXXXX")
 // properties.put("keystore.password","changeit")
    Class.forName("com.ibm.db2.jcc.DB2Driver")
    */
    
    
 
 

  val s = spark.read.jdbc(url,table,properties)


  val d1 = s.createOrReplaceTempView("Hi1")   //CE05




val s1 = spark.sql("select count(*) from Hi1") 

 s1.show()


   }
}










 