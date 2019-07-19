package com.ibm.fms.bim.util

import scala.io.Source
import java.util.ResourceBundle
import util.control.Breaks._
import org.apache.log4j.Logger
//import org.apache.logging.log4j.LogManager
class SqlReader() {

  
  val logger = Logger.getLogger("SqlReader.class");
  def getSqlFromFile(sqlfile: String, args: String*): String = {

    val file = Source.fromFile(this.getClass().getClassLoader.getResource(sqlfile).getPath());

    val resource = ResourceBundle.getBundle("config")

    var sql: String = ""

    val arglen=args.length
    var courser:Int=0
    for (line <- file.getLines) {
      breakable {
        if (line.indexOf("--") >= 0)
          break
        var newline = line
        while (newline.indexOf("$") >= 0) {
          val param = newline.substring(newline.indexOf("$") + 2, newline.indexOf("}"))

          newline = newline.replace("${" + param + "}", resource.getString(param))
        }
        
        while (newline.indexOf("?") >= 0) {
          newline = newline.replaceFirst("\\?", args(courser))
          courser+=1
        }
        
        sql = sql.concat(newline).concat(" ")
        
        
      }
    }
    file.close
    
   logger.debug(sql); 
    return sql;
  }
}