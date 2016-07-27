package dataModels

trait BookingDataWriter {
  import java.sql.{Connection, PreparedStatement, ResultSet}
  import com.mongodb.casbah.Imports._
  import com.mongodb.casbah.WriteConcern

  def writeMongoBookingDump(coll: MongoCollection, obj: BookingDumpBSON) =  {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.insert(obj.asBSONObject, wc)
  }
  
  def writeMongoBookingDumpSnapshot(coll: MongoCollection, obj: BookingDumpSnapshotBSON) =  {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.insert(obj.asBSONObject, wc)
  }
  
  def writeSQLBookingDump(conn: Connection, param: BookingDump): Unit = {
    val pst: PreparedStatement = conn.prepareStatement("""
      |INSERT INTO booking_dump 
      |(ID, At_Attach, Account_Name, Customer_Name, ERP_Deal_ID, Sales_Order_Number_Detail,
      |FP_Year, FP_Quarter, FP_Month, FP_Week, Partner, Partner_Name, TBM, Region, Sales_Level_6,
      |SCMS, Sub_SCMS_incorrect, TMS_Level_1_Sales_Allocated, Tech_Name, Tech_Code, Technology_Group,
      |Partner_Tier_Code, Ship_To_City, Booking_Net, Base_List, TMS_Sales_Allocated_Bookings_Quantity,
      |Internal_Business_Entity_Name, Internal_Sub_Business_Entity_Name, arch1, arch2, Product_ID,
      |Bill_To_Site_City, Vertical, iot_portfolio, GTMu, Product_Family, Booking_Adjustment, Partner_Certification,
      |Partner_Type, unique_state, prod_ser, standard_cost, Bookings_Adjustments_Code, Bookings_Adjustments_Description, CBN_Flag, Sub_SCMS)
      |VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin.replaceAll("\n", " "))
    pst.setInt(1, param.id)
    pst.setString(2, param.atAttach)
    pst.setString(3, param.accountName)
    pst.setString(4, param.customerName)
    pst.setString(5, param.erpDealID)
    pst.setString(6, param.soNumber)
    pst.setInt(7, param.fpYear)
    pst.setString(8, param.fpQuarter)
    pst.setInt(9, param.fpMonth)
    pst.setInt(10, param.fpWeek)
    pst.setString(11, param.partner)
    pst.setString(12, param.partnerName)
    pst.setString(13, param.salesAgent)
    pst.setString(14, param.region)
    pst.setString(15, param.sl6)
    pst.setString(16, param.scms)
    pst.setString(17, param.subSCMSIncorrect)
    pst.setString(18, param.tmsL1)
    pst.setString(19, param.techName)
    pst.setString(20, param.techCode)
    pst.setString(21, param.technologyGroup)
    pst.setString(22, param.partnerTierCode)
    pst.setString(23, param.shipToCity)
    pst.setDouble(24, param.bookingNet)
    pst.setDouble(25, param.baseList)
    pst.setString(26, param.bookingQty)
    pst.setString(27, param.beName)
    pst.setString(28, param.subBEName)
    pst.setString(29, param.arch1)
    pst.setString(30, param.arch2)
    pst.setString(31, param.productID)
    pst.setString(32, param.billToCity)
    pst.setString(33, param.vertical)
    pst.setString(34, param.iotPortfolio)
    pst.setString(35, param.gtmu)
    pst.setString(36, param.productFamily)
    pst.setString(37, param.bookingAdjustment)
    pst.setString(38, param.partnerCertification)
    pst.setString(39, param.partnerType)
    pst.setString(40, param.uniqueState)
    pst.setString(41, param.prodSer)
    pst.setDouble(42, param.stdCost)
    pst.setString(43, param.BookingsAdjustmentsCode)
    pst.setString(44, param.bookingsAdjustmentsDescription)
    pst.setString(45, param.cbnFlag)
    pst.setString(46, param.subSCMS)
    pst.executeUpdate()
  }

}