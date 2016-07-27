package connUtil.sql

import java.sql.{Connection, DriverManager}

object DBConnection {
  
  private val SQL_REGISTER_STRING: String = "com.mysql.jdbc.Driver"
  private val URL: String = "jdbc:mysql://localhost:3306/mysourcedata"
  private val USR: String = "test"
  private val PWD: String = "test@123"
  
  def apply() = new DBConnection
}

class DBConnection {
  private val props = Map(
      "register_string" -> DBConnection.SQL_REGISTER_STRING,
      "url" -> DBConnection.URL,
      "usr" -> DBConnection.USR,
      "pwd" -> DBConnection.PWD
  )
  def getConnection(): Connection = {
    var connection: Connection = null
    try {
      Class.forName(props("register_string")).newInstance()
      connection = DriverManager.getConnection(props("url"), props("usr"), props("pwd"))
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    connection
  }
}