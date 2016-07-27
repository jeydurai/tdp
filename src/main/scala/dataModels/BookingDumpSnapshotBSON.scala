package dataModels

import com.mongodb.casbah.Imports._


case class BookingDumpSnapshotBSON(
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
      val standardCost: Double
) {

  def asBSONObject(): MongoDBObject = {
    val obj: MongoDBObject = MongoDBObject(
      "periods" -> MongoDBObject(
        "year" -> fpYear.toString(),
        "quarter" -> fpQuarter,
        "month" -> fpMonth.toString(),
        "week" -> fpWeek.toString()
      ),
      "names" -> MongoDBObject(
        "partner" -> MongoDBObject(
          "name" -> partner,
          "unique_name" -> partnerName,
          "tier_code" -> partnerTierCode,
          "certification" -> partnerCertification
        ),
        "customer" -> MongoDBObject(
          "name" -> accountName,
          "unique_name" -> customerName
        ),
        "sales_agent" -> MongoDBObject(
          "name" -> salesAgent
        )
      ),
      "technologies" -> MongoDBObject(
        "tech_name" -> techName,
        "arch1" -> arch1,
        "arch2" -> arch2,
        "at_attach" -> atAttach,
        "iot_portfolio" -> iotPortfolio
      ),
      "business_nodes" -> MongoDBObject(
        "scms" -> scms,
        "sub_scms" -> subSCMS,
        "industry_vertical" -> vertical
      ),
      "location_nodes" -> MongoDBObject(
        "gtmu" -> gtmu,
        "region" -> region,
        "sales_level_6" -> sl6,
        "unique_state" -> uniqueState
      ),
      "references" -> MongoDBObject(
        "erp_deal_id" -> erpDealID,
        "sales_order_number_detail" -> soNumber
      ),
      "metric" -> MongoDBObject(
        "booking_net" -> bookingNet,
        "base_list" -> baseList,
        "standard_cost" -> standardCost
      ),
      "prod_ser" -> prodSer
    )
    obj
  }
  
}