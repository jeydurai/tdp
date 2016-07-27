package connUtil.sql

import java.sql.{Connection, DriverManager}
import org.apache.commons.dbcp2._

object DBPooledConnection {
  private val SQL_REGISTER_STRING: String = "com.mysql.jdbc.Driver"
  private val URL: String = "jdbc:mysql://localhost:3306/mysourcedata"
  private val USR: String = "test"
  private val PWD: String = "test@123"
  
  def apply() = new DBPooledConnection
  
}

class DBPooledConnection {
  private val props = Map(
      "register_string" -> DBPooledConnection.SQL_REGISTER_STRING,
      "url" -> DBPooledConnection.URL,
      "usr" -> DBPooledConnection.USR,
      "pwd" -> DBPooledConnection.PWD
  )
  def getDataSource(): BasicDataSource = {
    var connectionPool = new BasicDataSource()
    connectionPool.setUsername(DBPooledConnection.USR)
    connectionPool.setPassword(DBPooledConnection.PWD)
    connectionPool.setDriverClassName(DBPooledConnection.SQL_REGISTER_STRING)
    connectionPool.setUrl(DBPooledConnection.URL)
    connectionPool.setInitialSize(3)
    connectionPool
  }
}