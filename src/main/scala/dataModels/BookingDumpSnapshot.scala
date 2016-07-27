package dataModels

import java.sql.Connection


case class BookingDumpSnapshot(
      val atAttach: String,
      val accountName: String,
      val customerName: String,
      val erpDealID: String,
      val soNumber: String,
      val fpYear: String,
      val fpQuarter: String,
      val fpMonth: String,
      val fpWeek: String,
      val partner: String,
      val partnerName: String,
      val salesAgent: String,
      val region: String,
      val sl6: String,
      val uniqueState: String,
      val scms: String,
      val subSCMS: String,
      val techName: String,
      val partnerTierCode: String,
      val arch1: String,
      val arch2: String,
      val vertical: String,
      val iotPortfolio: String,
      val gtmu: String,
      val partnerCertification: String,
      val prodSer: String,
      val bookingNet: Double,
      val baseList: Double,
      val standardCost: Double,
      val sqlConn: Connection
    
) {
  
}