package dataModels

import dataModels._
import java.sql.Connection

case class DumpFromFinance(
      val id: Int, 
      val bookingsAdjustmentsCode: String, 
      val bookingsAdjustmentsDescription: String,
      val bookingsAdjustmentsType: String,
      val atAttach: String,
      val businessUnit: String,
      val accountName: String,
      val erpDealId: String,
      val soNumber: String,
      val fiscalPeriodId: String,
      val fiscalQuarterId: String,
      val fiscalWeekId: String,
      val partner: String,
      val productFamily: String,
      val productId: String,
      val salesAgent: String,
      val salesLevel2: String,
      val salesLevel3: String,
      val salesLevel4: String,
      val salesLevel5: String,
      val salesLevel6: String,
      val scms: String,
      val subSCMS: String,
      val tmsLevel1: String,
      val tmsLevel2: String,
      val tmsLevel3: String,
      val tmsLevel4: String,
      val technologyGroup: String,
      val partnerTierCode: String,
      val partnerCertification: String,
      val partnerType: String,
      val billToSiteCity: String,
      val shipToCity: String,
      val cbnFlag: String,
      val bookingNet: Double,
      val baseList: Double,
      val beName: String,
      val subBEName: String,
      val stdCost: Double,
      val prodSer: String,
      val servicesIndicator: String,
      val conn: Connection
    )  extends BookingDataFieldAdder{
  
  def asBookingDumpData(): Tuple2[BookingDump, BookingDumpBSON] = {
    val periodYear: Int = fiscalPeriodId take 4 toInt
    val periodMonth: Int = fiscalPeriodId takeRight 2 toInt
    val periodQuarter: String = fiscalQuarterId takeRight 2
    val weekFieldLastTwo: String = fiscalWeekId takeRight 2
    val periodWeek: Int = getWeek(conn, periodQuarter, weekFieldLastTwo).toInt
    val (customerName, vertical): Tuple2[String, String] = getUniqueCustomerVertical(conn, accountName)
    val partnerName: String = getUniquePartner(conn, partner)
    val uniqueState: String = getUniqueState(conn, salesLevel6)
    val (gtmu, region): Tuple2[String, String] = getUniqueRegionAndGTMu(conn, salesLevel5)
    val clnSubSCMS: String = cleanSubSCMS(subSCMS)
    val correctSubSCMS: String = this.getCorrectSubSCMS(conn, customerName, periodYear, salesLevel6)
    val techCode: String = extractTechCode(conn, periodYear, tmsLevel1, subBEName)
    val (techName, arch1, arch2): Tuple3[String, String, String] = getTechnologies(conn, techCode)
    val iotPortfolio: String = getIOTPortfolio(conn, productFamily, productId)
    val bookingQty = ""
    val bookingDumpSQLData: BookingDump = new BookingDump(
      id, atAttach, accountName, customerName, erpDealId, soNumber, periodYear, periodQuarter, periodMonth,
      periodWeek, partner, partnerName, salesAgent, region, salesLevel6, scms, subSCMS, correctSubSCMS, tmsLevel1, techName,
      techCode, technologyGroup, partnerTierCode, shipToCity, bookingNet, baseList, bookingQty, 
      beName, subBEName, arch1, arch2, productId, billToSiteCity, vertical, iotPortfolio, gtmu, 
      partnerCertification, partnerType, productFamily, bookingsAdjustmentsType, uniqueState, prodSer, stdCost,
      bookingsAdjustmentsCode, bookingsAdjustmentsDescription, cbnFlag
    )
    val bookingDumpMongoData: BookingDumpBSON = new BookingDumpBSON(
      id, atAttach, accountName, customerName, erpDealId, soNumber, periodYear, periodQuarter, periodMonth,
      periodWeek, partner, partnerName, salesAgent, region, salesLevel6, scms, correctSubSCMS, tmsLevel1, techName,
      techCode, technologyGroup, partnerTierCode, shipToCity, bookingNet, baseList, bookingQty, 
      beName, subBEName, arch1, arch2, productId, billToSiteCity, vertical, iotPortfolio, gtmu, 
      partnerCertification, partnerType, productFamily, bookingsAdjustmentsType, uniqueState, prodSer, stdCost,
      bookingsAdjustmentsCode, bookingsAdjustmentsDescription, cbnFlag, tmsLevel2, tmsLevel3, salesLevel3, salesLevel4,
      salesLevel5, businessUnit
    )
    //conn.close
    val bookingDump: Tuple2[BookingDump, BookingDumpBSON] = (bookingDumpSQLData, bookingDumpMongoData)
    bookingDump
  }
}