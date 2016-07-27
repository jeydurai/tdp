package dataModels

import com.mongodb.casbah.Imports._


case class BookingDumpBSON(
    val id: Int,
    val atAttach: String,
    val accountName: String,
    val customerName: String,
    val erpDealID: String,
    val soNumber: String,
    val fpYear: Int,
    val fpQuarter: String,
    val fpMonth: Int,
    val fpWeek: Int,
    val partner: String,
    val partnerName: String,
    val salesAgent: String,
    val region: String,
    val sl6: String,
    val scms: String,
    val subSCMS: String,
    val tmsL1: String,
    val techName: String,
    val techCode: String,
    val technologyGroup: String,
    val partnerTierCode: String,
    val shipToCity: String,
    val bookingNet: Double,
    val baseList: Double,
    val bookingQty: String,
    val beName: String,
    val subBEName: String,
    val arch1: String,
    val arch2: String,
    val productID: String,
    val billToCity: String,
    val vertical: String,
    val iotPortfolio: String,
    val gtmu: String,
    val partnerCertification: String,
    val partnerType: String,
    val productFamily: String,
    val bookingAdjustment: String,
    val uniqueState: String,
    val prodSer: String,
    val stdCost: Double,
    val bookingAdjustmentCode: String,
    val bookingAdjustmentDescription: String,
    val cbnFlag: String,
    val tmsL2: String,
    val tmsL3: String,
    val sl3: String,
    val sl4: String,
    val sl5: String,
    val businessUnit: String
) {
  
  def asBSONObject(): MongoDBObject = {
    val obj: MongoDBObject = MongoDBObject(
      "booking_adjustments" -> MongoDBObject(
        "bookings_adjustments_code" -> bookingAdjustmentCode,
        "bookings_adjustments_description" -> bookingAdjustmentDescription,
        "bookings_adjustments_type" -> bookingAdjustment,
        "cbn_flag" -> cbnFlag
      ),
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
          "certification" -> partnerCertification,
          "type" -> partnerType
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
        "product_family" -> productFamily,
        "product_id" -> productID,
        "technology_group" -> technologyGroup,
        "tms_level_1_sales_allocated" -> tmsL1,
        "tms_level_2_sales_allocated" -> tmsL2,
        "tms_level_3_sales_allocated" -> tmsL3,
        "internal_business_entity_name" -> beName,
        "internal_sub_business_entity_name" -> subBEName,
        "tech_name" -> techName,
        "arch1" -> arch1,
        "arch2" -> arch2,
        "at_attach" -> atAttach,
        "iot_portfolio" -> iotPortfolio
      ),
      "business_nodes" -> MongoDBObject(
        "sales_level_3" -> sl3,
        "scms" -> scms,
        "sub_scms" -> subSCMS,
        "business_unit" -> businessUnit,
        "industry_vertical" -> vertical
      ),
      "location_nodes" -> MongoDBObject(
        "sales_level_4" -> sl4,
        "gtmu" -> gtmu,
        "sales_level_5" -> sl5,
        "region" -> region,
        "sales_level_6" -> sl6,
        "unique_state" -> uniqueState,
        "bill_to_site_city" -> billToCity,
        "ship_to_city" -> shipToCity
      ),
      "references" -> MongoDBObject(
        "erp_deal_id" -> erpDealID,
        "sales_order_number_detail" -> soNumber
      ),
      "metric" -> MongoDBObject(
        "booking_net" -> bookingNet,
        "base_list" -> baseList,
        "standard_cost" -> stdCost,
        "bookings_quantity" -> bookingQty
      ),
      "prod_ser" -> prodSer
    )
    obj
  }
}