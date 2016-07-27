package dataModels

import java.sql.{Connection, PreparedStatement, ResultSet}

trait MiscSQLWriter {

  def writeTechMaster(conn: Connection): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""DELETE FROM tech_master""".stripMargin.replaceAll("\n", " "))
    pst.execute()    
    val pst2: PreparedStatement = conn.prepareStatement("""INSERT INTO tech_master SELECT DISTINCT 
        |RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) AS Tech_Code, 
        |IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec AS ts 
        |WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts.tech_code),'Others') AS Tech_Name_1, 
        |IFNULL((SELECT ts2.arch1 FROM tech_spec AS ts2 
        |WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts2.tech_code),'Other') AS arch1, 
        |IFNULL((SELECT ts3.arch2 FROM tech_spec AS ts3 
        |WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts3.tech_code),'Others') AS arch2 
        |FROM dump_from_finance AS df ORDER BY tech_code""".stripMargin.replaceAll("\n", " "))
    pst2.execute()    
  }

  def writeTechMaster1(conn: Connection): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""DELETE FROM tech_master1""".stripMargin.replaceAll("\n", " "))
    pst.execute()    
    val pst2: PreparedStatement = conn.prepareStatement("""INSERT INTO tech_master1 SELECT DISTINCT Internal_Sub_Business_Entity_Name AS Tech_Code, 
        |IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec1 AS ts WHERE Internal_Sub_Business_Entity_Name = ts.tech_code),'Others') AS Tech_Name_1, 
        |IFNULL((SELECT ts2.arch1 FROM tech_spec1 AS ts2 WHERE Internal_Sub_Business_Entity_Name = ts2.tech_code),'Others') AS arch1, 
        |IFNULL((SELECT ts3.arch2 FROM tech_spec1 AS ts3 WHERE Internal_Sub_Business_Entity_Name = ts3.tech_code),'Others') AS arch2	
        |FROM dump_from_finance AS df ORDER BY tech_code""".stripMargin.replaceAll("\n", " "))
    pst2.execute()    
  }

  def writeGrandTechMaster(conn: Connection): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""DELETE FROM tech_grand_master""".stripMargin.replaceAll("\n", " "))
    pst.execute()    
    val pst2: PreparedStatement = conn.prepareStatement("""INSERT INTO mysourcedata.tech_grand_master SELECT * FROM mysourcedata.tech_master 
        |UNION ALL SELECT * FROM mysourcedata.tech_master1""".stripMargin.replaceAll("\n", " "))
    pst2.execute()    
  }

  def dropBookingDumpTables(conn: Connection): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""DROP TABLE booking_dump""".stripMargin.replaceAll("\n", " "))
    pst.execute()    
    val pst2: PreparedStatement = conn.prepareStatement("""DROP TABLE booking_dump_nri""".stripMargin.replaceAll("\n", " "))
    pst2.execute()    
  }
  
  def recreateBookingDumpTables(conn: Connection): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""CREATE TABLE booking_dump LIKE booking_dump_template""".stripMargin.replaceAll("\n", " "))
    pst.execute()    
    val pst2: PreparedStatement = conn.prepareStatement("""CREATE TABLE booking_dump_nri LIKE booking_dump_template""".stripMargin.replaceAll("\n", " "))
    pst2.execute()    
  }
    
    
}