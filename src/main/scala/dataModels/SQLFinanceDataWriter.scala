package dataModels

import connUtil.sql.{DBConnection, DBPooledConnection}
import connUtil.nosql.{MongoConnection}
import java.sql.{PreparedStatement, ResultSet}
import java.text.DecimalFormat

object SQLFinanceDataWriter extends BookingDataWriter with MiscSQLWriter{

  def BookingDataInit(): Boolean = {
    val dataSource = DBPooledConnection().getDataSource()
    val sqlConn = dataSource.getConnection
    println("Initialization process is on, which will take several minute(s)...........")
    println("Writing Tech Master...")
    writeTechMaster(sqlConn)
    println("Tech Master has been successfully written!")
    println("Writing Tech Master1...")
    writeTechMaster1(sqlConn)
    println("Tech Master1 has been successfully written!")
    println("Writing Grand Tech Master...")
    writeGrandTechMaster(sqlConn)
    println("Grand Tech Master has been successfully written!")
    println("Dropping 'booking_dump' and 'booking_dump_nri' tables... ")
    dropBookingDumpTables(sqlConn)
    println("'booking_dump' and 'booking_dump_nri' tables have been dropped successfully!")
    println("Recreating 'booking_dump' and 'booking_dump_nri' tables... ")
    recreateBookingDumpTables(sqlConn)
    println("'booking_dump' and 'booking_dump_nri' tables have been recreated successfully!")
    sqlConn.close
    println("Initialization process is complete!")
    true
  }
  
  def convertAndWrite(): Boolean = {
    val dataSource = DBPooledConnection().getDataSource()
    val sqlConn = dataSource.getConnection
    val mongoColl = MongoConnection().getTrueNorthBookingDump()
    mongoColl.drop
    val pstDumpFromFinance: PreparedStatement = sqlConn.prepareStatement("""
    |SELECT * FROM dump_from_finance WHERE Booking_Net <> 0 
    """.stripMargin.replaceAll("\n", " "))
    println("Querying 'mysourcedata.dump_from_finance' ...")
    val resultSet: ResultSet = pstDumpFromFinance.executeQuery()
    resultSet.beforeFirst(); resultSet.last();
    val totalRecs: Int = resultSet.getRow
    resultSet.beforeFirst()
    var rowCounter: Int = 0
    val startTime: Double = System.currentTimeMillis()
    while (resultSet.next()) {
      val financeData: DumpFromFinance = new DumpFromFinance(
      resultSet.getInt("ID"),
      resultSet.getString("Bookings_Adjustments_Code"),
      resultSet.getString("Bookings_Adjustments_Description"),
      resultSet.getString("Bookings_Adjustments_Type"),
      resultSet.getString("AT_Attach"),
      resultSet.getString("Business_Unit"),
      resultSet.getString("Customer_Name"),
      resultSet.getString("ERP_Deal_ID"),
      resultSet.getString("Sales_Order_Number_Detail"),
      resultSet.getString("Fiscal_Period_ID"),
      resultSet.getString("Fiscal_Quarter_ID"),
      resultSet.getString("Fiscal_Week_ID"),
      resultSet.getString("Partner_Name"),
      resultSet.getString("Product_Family"),
      resultSet.getString("Product_ID"),
      resultSet.getString("TBM"),
      resultSet.getString("Sales_Level_2"),
      resultSet.getString("Sales_Level_3"),
      resultSet.getString("Sales_Level_4"),
      resultSet.getString("Sales_Level_5"),
      resultSet.getString("Sales_Level_6"),
      resultSet.getString("SCMS"),
      resultSet.getString("Sub_SCMS"),
      resultSet.getString("TMS_Level_1_Sales_Allocated"),
      resultSet.getString("TMS_Level_2_Sales_Allocated"),
      resultSet.getString("TMS_Level_3_Sales_Allocated"),
      resultSet.getString("TMS_Level_4_Sales_Allocated"),
      resultSet.getString("Technology_Group"),
      resultSet.getString("Partner_Tier_Code"),
      resultSet.getString("Partner_Certification"),
      resultSet.getString("Partner_Type"),
      resultSet.getString("Bill_To_Site_City"),
      resultSet.getString("Ship_To_City"),
      resultSet.getString("CBN_Flag"),
      resultSet.getDouble("Booking_Net"),
      resultSet.getDouble("TMS_Sales_Allocated_Bookings_Base_List"),
      resultSet.getString("Internal_Business_Entity_Name"),
      resultSet.getString("Internal_Sub_Business_Entity_Name"),
      resultSet.getDouble("standard_cost"),
      resultSet.getString("prod_ser"),
      resultSet.getString("services_indicator"),
      sqlConn
      )
      val (bookingDumpSQLData, bookingDumpMongoData): Tuple2[BookingDump, BookingDumpBSON] = financeData.asBookingDumpData
      writeSQLBookingDump(sqlConn, bookingDumpSQLData)
      writeMongoBookingDump(mongoColl, bookingDumpMongoData)
      rowCounter += 1
      val formatter = new DecimalFormat("#.##")
      val percentComplete: Float = (rowCounter.toFloat/totalRecs.toFloat)*100
      //println("(" + rowCounter.toString + "/" + rowCounter.toString + ") " + " Customer Name: " + bookingDump.customerName)
      //print("(" + rowCounter.toString + "/" + totalRecs.toString + ") ")
      var endTime: Double = System.currentTimeMillis()
      var totalSecs: Double = (endTime-startTime)/1000
      var totalMins: Double = totalSecs/60
      var totalHours: Double = totalMins/60
      if (rowCounter.toFloat%100.toFloat == 0.0) {
        print("Processing ... " + formatter.format(percentComplete) + "% (" + rowCounter.toString + "/" + totalRecs.toString + ") ")
        print("Time Elapsed: " + formatter.format(totalHours) + "Hr(s) " + formatter.format(totalMins) + "min(s) " + formatter.format(totalSecs) + "secs(s) ")
        print("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b")
        print("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b")

      }
    }
    println("\nProcess Completed!")
    resultSet.close
    pstDumpFromFinance.close
    sqlConn.close
    true
  }

  def convertAsSnapshotAndWrite(): Boolean = {
    val dataSource = DBPooledConnection().getDataSource()
    val sqlConn = dataSource.getConnection
    val mongoColl = MongoConnection().getTrueNorthBookingDump2()
    mongoColl.drop
    println("Obtaining Latest Year...")
    val pstMaxYear: PreparedStatement = sqlConn.prepareStatement("SELECT MAX(FP_Year) As max_year FROM booking_dump")
    val resultSetMaxYear: ResultSet = pstMaxYear.executeQuery
    val maxYear: Int = resultSetMaxYear match {
      case my => {
        var year: Int = 0
        while (my.next()) {
          year = my.getInt("max_year")
        }
        year
      }
    }
    println("Latest Year has been obtained!")
    println("Obtaining Latest Month using Latest year...")

    val pstMaxMonth: PreparedStatement = sqlConn.prepareStatement("SELECT MAX(FP_Month) AS max_month FROM booking_dump WHERE FP_Year=?")
    pstMaxMonth.setInt(1, maxYear)
    val resultSetMaxMonth: ResultSet = pstMaxMonth.executeQuery
    val maxMonth: Int = resultSetMaxMonth match {
      case my => {
        var month: Int = 0
        while (my.next()) {
          month = my.getInt("max_month")
        }
        month
      }
    }
    println("Latest Month has been acquired!")
    println("Fetching booking_dump subset...")
    
    val pstDumpFromFinance: PreparedStatement = sqlConn.prepareStatement("""
    |SELECT DISTINCT AT_Attach, Account_Name, Customer_Name, ERP_Deal_ID, Sales_Order_Number_Detail, FP_Year, 
    |FP_Quarter, FP_Month, FP_Week, partner, Partner_Name, TBM, region, Sales_Level_6, unique_state, SCMS,
    |Sub_SCMS, Tech_Name, Partner_Tier_Code, arch1, arch2, Vertical, iot_portfolio, GTMu, Partner_Certification,
    |prod_ser, SUM(Booking_Net) AS Booking_Net, SUM(Base_List) As Base_List, SUM(standard_cost) As standard_cost
    |FROM mysourcedata.booking_dump WHERE FP_Month <= ? GROUP BY AT_Attach, Account_Name, Customer_Name, ERP_Deal_ID, Sales_Order_Number_Detail,
    |FP_Year, FP_Quarter, FP_Month, FP_Week, partner, Partner_Name, TBM, region, Sales_Level_6, unique_state, SCMS, Sub_SCMS,
    |Tech_Name, Partner_Tier_Code, arch1, arch2, Vertical, iot_portfolio, GTMu, Partner_Certification, prod_ser 
    """.stripMargin.replaceAll("\n", " "))
    println("Querying 'mysourcedata.booking_dump' ...")
    pstDumpFromFinance.setInt(1, maxMonth)
    val resultSet: ResultSet = pstDumpFromFinance.executeQuery
    resultSet.beforeFirst(); resultSet.last();
    val totalRecs: Int = resultSet.getRow
    resultSet.beforeFirst()
    var rowCounter: Int = 0
    val startTime: Double = System.currentTimeMillis()
    println("Fetched booking_dump subset! (" + totalRecs.toString + ")")
    while (resultSet.next()) {
      val snapShotData: BookingDumpSnapshotBSON = new BookingDumpSnapshotBSON(
      resultSet.getString("AT_Attach"),
      resultSet.getString("Account_Name"),
      resultSet.getString("Customer_Name"),
      resultSet.getString("ERP_Deal_ID"),
      resultSet.getString("Sales_Order_Number_Detail"),
      resultSet.getString("FP_Year"),
      resultSet.getString("FP_Quarter"),
      resultSet.getString("FP_Month"),
      resultSet.getString("FP_Week"),
      resultSet.getString("partner"),
      resultSet.getString("Partner_Name"),
      resultSet.getString("TBM"),
      resultSet.getString("region"),
      resultSet.getString("Sales_Level_6"),
      resultSet.getString("unique_state"),
      resultSet.getString("SCMS"),
      resultSet.getString("Sub_SCMS"),
      resultSet.getString("Tech_Name"),
      resultSet.getString("Partner_Tier_Code"),
      resultSet.getString("arch1"),
      resultSet.getString("arch2"),
      resultSet.getString("Vertical"),
      resultSet.getString("iot_portfolio"),
      resultSet.getString("GTMu"),
      resultSet.getString("Partner_Certification"),
      resultSet.getString("prod_ser"),
      resultSet.getDouble("Booking_Net"),
      resultSet.getDouble("Base_List"),
      resultSet.getDouble("standard_cost")
      )
      writeMongoBookingDumpSnapshot(mongoColl, snapShotData)
      rowCounter += 1
      val formatter = new DecimalFormat("#.##")
      val percentComplete: Float = (rowCounter.toFloat/totalRecs.toFloat)*100
      var endTime: Double = System.currentTimeMillis()
      var totalSecs: Double = (endTime-startTime)/1000
      var totalMins: Double = totalSecs/60
      var totalHours: Double = totalMins/60
      if (rowCounter.toFloat%100.toFloat == 0.0) {
        print("Processing ... " + formatter.format(percentComplete) + "% (" + rowCounter.toString + "/" + totalRecs.toString + ") ")
        print("Time Elapsed: " + formatter.format(totalHours) + "Hr(s) " + formatter.format(totalMins) + "min(s) " + formatter.format(totalSecs) + "secs(s) ")
        print("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b")
        print("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b")

      }
    }
    println("\nProcess Completed!")
    resultSet.close
    pstDumpFromFinance.close
    sqlConn.close
    true
  }

}