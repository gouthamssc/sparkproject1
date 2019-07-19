package com.ibm.fms.bim.util
import java.util.Properties
import java.sql.{ Connection, DriverManager, ResultSet }
import java.util.ResourceBundle
import com.ibm.db2.jcc._

class JdbcUtil(DBPrefix: String) {
  //

  var resource = ResourceBundle.getBundle("config")

  //
  def getUrl(): String = {
  //  if(DBPrefix.endsWith("DB2"))    
        return "jdbc:db2://" + resource.getString(DBPrefix + ".host") + ":" + resource.getString(DBPrefix + ".port") + "/" + resource.getString(DBPrefix + ".database");
  }

  def getConProperties(): Properties = {

    val connectionProperties = new Properties()
    connectionProperties.put("user", resource.getString(DBPrefix + ".user"))
    connectionProperties.put("password", resource.getString(DBPrefix + ".password"))
    if (resource.getString(DBPrefix + ".ssl") == "true") {
      connectionProperties.put("sslConnection", "true")
      connectionProperties.put("sslTrustStoreLocation", this.getClass().getClassLoader.getResource(resource.getString(DBPrefix + ".sslKey")).getPath())

    }

    return connectionProperties;

  }

  def executeSQL(sql: String) {
    classOf[com.ibm.db2.jcc.DB2Driver]
    val conn = DriverManager.getConnection(getUrl(),getConProperties())
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    try {
      val prep = conn.prepareStatement(sql)
      prep.executeUpdate
    } finally {
      conn.close
    }
  }
  
  def loadTable(stmt: String,data: String) {
    classOf[com.ibm.db2.jcc.DB2Driver]
    val conn = DriverManager.getConnection(getUrl(),getConProperties()).asInstanceOf[DB2Connection]
    
    //https://www.ibm.com/support/knowledgecenter/SSEPEK_10.0.0/sqlref/src/tpc/db2z_sp_adminutlexecute.html
    conn.prepareCall("{SYSPROC.ADMIN_COMMAND_DB2()}") 
    
    
    //jdbc zload only suit for z/os db2 12 or later    
    //conn.zLoad(stmt, data, "xxxx")
    
    
    
    //sysproc.ADMIN_CMD is only suit for LUW DB2
//   https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.java.doc/src/tpc/imjcc_tjv00027.html
   
  }
}