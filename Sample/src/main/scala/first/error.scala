package first

import java.util.Properties
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import scala.io.Source



object error {
  
  def main(args: Array[String]) = {

    
  val  spark = SparkSession.builder()
    .master("local")
    .appName("SQLCNCTN" )
    .getOrCreate()

	  
	 
   val url = "jdbc:db2://9.212.130.127:38362/CE05CDA1"
  val schema = "DBDFMSDR"
  
   val properties = new Properties()
    properties.put("user","IAVP3AG")
    properties.put("password","GOUT1234")
   properties.put("sslConnection", "true");
   properties.put("sslTrustStoreLocation","/Users/gouthamrajanala/Documents/mopcloud-tls-truststore.jks")
 // properties.put("KeystoreLocation","/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/jre/lib/security/cacerts")
//   properties.put("sslTrustStorePassword","XXXXXX")
 // properties.put("keystore.password","changeit")
    Class.forName("com.ibm.db2.jcc.DB2Driver")

    print("hello")
  
  val sqlQuery = Source.fromFile("/Users/gouthamrajanala/Documents/new.sql").getLines.mkString 
print(sqlQuery)
  val results = properties.executeSQL("sqlQuery")
  
  results.show()
  
  
}
}