package dataModels.truenorth

import com.mongodb.casbah.Imports._

object MongoQueryObjects {

  // All Mongo Field Names
  // *********************
  // =======================================
  def yearFieldAsGroup: String = "$periods.year"
  def quarterFieldAsGroup: String = "$periods.quarter"
  def monthFieldAsGroup: String = "$periods.month"
  def weekFieldAsGroup: String = "$periods.week"
  def arch2FieldAsGroup: String = "$technologies.arch2"
  def verticalFieldAsGroup: String = "$business_nodes.industry_vertical"
  def techNameFieldAsGroup: String = "$technologies.tech_name"
  def atAttachFieldAsGroup: String = "$technologies.at_attach"
  def subSCMSFieldAsGroup: String = "$business_nodes.sub_scms"
  def gtmuFieldAsGroup: String = "$location_nodes.gtmu"
  def regionFieldAsGroup: String = "$location_nodes.region"
  def sl6FieldAsGroup: String = "$location_nodes.sales_level_6"
  def cusNameFieldAsGroup: String = "$names.customer.unique_name"
  def parNameFieldAsGroup: String = "$names.partner.unique_name"
  def soNumberFieldAsGroup: String = "$references.sales_order_number_detail"
  def prodSerFieldAsGroup: String = "$prod_ser"

  def yearField: String = "periods.year"
  def quarterField: String = "periods.quarter"
  def monthField: String = "periods.month"
  def weekField: String = "periods.week"
  def cusNameField: String = "names.customer.unique_name"
  def parNameField: String = "names.partner.unique_name"
  def soNumberField: String = "references.sales_order_number_detail"
  def subSCMSField: String = "business_nodes.sub_scms"
  def mappedToField: String = "mappedTo"
  
  def mongoSubSCMSObj(subSCMS: String): MongoDBObject = subSCMS match {
    case "SELECT" => MongoDBObject(subSCMSField -> "COMM_SELECT")
    case "MM" => MongoDBObject(subSCMSField -> "COM-MM")
    case "GEO_NAMED" => MongoDBObject(subSCMSField -> "COMM_GEO_NAMED")
    case "GEO_NON_NA" => MongoDBObject(subSCMSField -> "COMM_GEO_NON_NA")
    case "PL_S" | "PL_V" => MongoDBObject(subSCMSField -> "COMM_PL_S")
    case "PL" => MongoDBObject(subSCMSField -> "COMM_PL")
    case "OTHER" => MongoDBObject(subSCMSField -> "COM-OTHER")
    case x => MongoDBObject(subSCMSField -> x)
  }
  
  def multiSubSCMSQryObj(subSCMS: Option[String]): Option[List[DBObject]] = subSCMS match {
    case Some(v) if v equals "GEO_NON_NA" => Some(List(mongoSubSCMSObj(v), mongoSubSCMSObj("PL_S")))
    case _ => None
  }
  
  def customerNotQryObj(): List[MongoDBObject] = {
    List(
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^unknown".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^small busi".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^cobo una".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^run rate".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^runrate".r
              )
          ) //MongoObj
      ) //List
  }

  def partnerNotQryObj(): List[MongoDBObject] = {
    List(
          MongoDBObject(
              parNameField -> MongoDBObject(
                  "$not" -> "(?i)^unknown".r
              )
          ), //MongoObj
          MongoDBObject(
              parNameField -> MongoDBObject(
                  "$not" -> "(?i)^small busi".r
              )
          ), //MongoObj
          MongoDBObject(
              parNameField -> MongoDBObject(
                  "$not" -> "(?i)^cobo una".r
              )
          ), //MongoObj
          MongoDBObject(
              parNameField -> MongoDBObject(
                  "$not" -> "(?i)^run rate".r
              )
          ), //MongoObj
          MongoDBObject(
              parNameField -> MongoDBObject(
                  "$not" -> "(?i)^runrate".r
              )
          ) //MongoObj
    ) //List
  }
  
  def customerDealsNotQryObj(): List[MongoDBObject] = {
    List(
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^unknown".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^small busi".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^cobo una".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^run rate".r
              )
          ), //MongoObj
          MongoDBObject(
              cusNameField -> MongoDBObject(
                  "$not" -> "(?i)^runrate".r
              )
          ), //MongoObj
          MongoDBObject(
              soNumberField -> MongoDBObject(
                  "$ne" -> None
              )
          ), //MongoObj
          MongoDBObject(
              soNumberField -> MongoDBObject(
                  "$ne" -> ""
              )
          ) //MongoObj
      ) //List
  }
  def last3YearsQryObj(year: Option[Tuple3[String, String, String]]): List[DBObject] = 
    List(MongoDBObject(yearField -> year.get._1), MongoDBObject(yearField -> year.get._2), MongoDBObject(yearField -> year.get._3))
  
  // All Mongo Grouping for Unique Data
  // **********************************
  // ============================================
  def groupByPeriods(): DBObject = {
    val dbObj: DBObject = MongoDBObject(
        "$group" -> MongoDBObject(
            "_id" -> MongoDBObject(
                "fiscal_year" -> yearFieldAsGroup,
                "fiscal_quarter" -> quarterFieldAsGroup,
                "fiscal_month" -> monthFieldAsGroup,
                "fiscal_week" -> weekFieldAsGroup
            )
        )
    )
    dbObj
  }

  def groupByCustomers(): DBObject = {
    val dbObj: DBObject = MongoDBObject(
        "$group" -> MongoDBObject(
            "_id" -> cusNameFieldAsGroup
        )
    )
    dbObj
  }
  
  def groupByPartners(): DBObject = {
    val dbObj: DBObject = MongoDBObject(
        "$group" -> MongoDBObject(
            "_id" -> parNameFieldAsGroup
        )
    )
    dbObj
  }

  
  // All Mongo Group Sub Objects for Aggregation
  // *******************************************
  // ============================================
  def getGroupObjBooking(): MongoDBObject =
    MongoDBObject("$sum" -> "$metric.booking_net")

  def getGroupObjBaseList(): MongoDBObject =
    MongoDBObject("$sum" -> "$metric.base_list")
    
  // All Mongo Group Objects for Aggregation
  // *******************************************
  // ============================================
  def groupBookingByHistory(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> yearFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByArch2(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> arch2FieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingByVertical(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> verticalFieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingByTechName(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> techNameFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByAtAttach(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> atAttachFieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingBySubSCMS(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> subSCMSFieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingByGTMu(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> gtmuFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByRegion(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> regionFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingBySL6(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> sl6FieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingByCustomer(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> cusNameFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByCustomerSONumber(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> MongoDBObject("customers" -> cusNameFieldAsGroup, "soNumbers" -> soNumberFieldAsGroup), "booking" -> getGroupObjBooking))

  def groupBookingByCustomerTechologies(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> MongoDBObject("customers" -> cusNameFieldAsGroup, "techs" -> techNameFieldAsGroup), "booking" -> getGroupObjBooking))
    
  def groupBookingByPartner(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> parNameFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByQoQ(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> quarterFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByMoM(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> monthFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupBookingByWoW(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> weekFieldAsGroup, "booking" -> getGroupObjBooking))
    
  def groupBookingByProductService(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> prodSerFieldAsGroup, "booking" -> getGroupObjBooking))

  def groupExclusiveBookingNetAndList(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> None, "booking" -> getGroupObjBooking, "base_list" -> getGroupObjBaseList))
    
  def groupNetAndListByArchs(): DBObject = 
    MongoDBObject("$group" -> MongoDBObject("_id" -> arch2FieldAsGroup, "booking" -> getGroupObjBooking, "base_list" -> getGroupObjBaseList))
    
    
  // All Mongo Match Objects for Aggregation
  // *******************************************
  // ============================================
  
  def matchObjByPeriodsCustomerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> customerNotQryObj)))
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerNotQryObj)))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj)))
  }    

  
  def matchObjByPeriodsCustomerNotLast3Years(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), "$and" -> customerNotQryObj)))
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "$and" -> customerNotQryObj)))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj)))
  }    
  
  def matchObjByPeriodsCustomerDealsNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> customerDealsNotQryObj))) //Some
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerDealsNotQryObj)))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerDealsNotQryObj)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerDealsNotQryObj)))
  }    

  
  
  def matchObjByPeriodsPartnerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> partnerNotQryObj)))
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> partnerNotQryObj)))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> partnerNotQryObj)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> partnerNotQryObj)))
  }    
  

  
  // Prod_Ser Methods
  // 888888888888888888888888888888888888888//
    def matchObjByProdSerCustomerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get,"$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
  }    

  
  def matchObjByProdSerCustomerNotLast3Years(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), "$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get )))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get )))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, "prod_ser" -> ps.get)))
  }    
  
  
  def matchObjByProdSerCustomerDealsNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> customerDealsNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerDealsNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerDealsNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerDealsNotQryObj, "prod_ser" -> ps.get)))
  }    


  def matchObjByProdSerPartnerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> partnerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> partnerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, 
                                    "$and" -> partnerNotQryObj, "prod_ser" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> partnerNotQryObj, "prod_ser" -> ps.get)))
  }    
  
  
  // SubSCMS Methods//
  // 888888888888888//
    def matchObjBySubSCMSCustomerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get,"$and" -> customerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get)))
  }    

  
  def matchObjBySubSCMSCustomerNotLast3Years(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), "$and" -> customerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get )))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get )))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, subSCMSField -> ps.get)))
  }    
  
  
  def matchObjBySubSCMSCustomerDealsNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> customerDealsNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerDealsNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerDealsNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerDealsNotQryObj, subSCMSField -> ps.get)))
  }    


  def matchObjBySubSCMSPartnerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> partnerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> partnerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> partnerNotQryObj, subSCMSField -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> partnerNotQryObj, subSCMSField -> ps.get)))
  }    
  
  // SubSCMS Special Methods//
  // 888888888888888//
    def matchObjBySpecialSubSCMSCustomerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get,"$and" -> customerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, "$or" -> ps.get)))
  }    

  
  def matchObjBySpecialSubSCMSCustomerNotLast3Years(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), "$and" -> customerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "$and" -> customerNotQryObj, "$or" -> ps.get )))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "$and" -> customerNotQryObj, "$or" -> ps.get )))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerNotQryObj, "$or" -> ps.get)))
  }    
  
  
  def matchObjBySpecialSubSCMSCustomerDealsNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> customerDealsNotQryObj, "$or" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> customerDealsNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> customerDealsNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> customerDealsNotQryObj, "$or" -> ps.get)))
  }    


  def matchObjBySpecialSubSCMSPartnerNot(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$and" -> partnerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$and" -> partnerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$and" -> partnerNotQryObj, "$or" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$and" -> partnerNotQryObj, "$or" -> ps.get)))
  }    

  
  
// General Mongo Periods Match Objects
// 88888888888888888888888888888888888888
  def matchObjectByPeriods(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, None, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get)))
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get)))
    case (u, None, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get)))
    case (u, None, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get)))
    case (u, None, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get)))
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get)))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get)))
  }

  def matchObjectByProdSer(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, None, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "prod_ser" -> ps.get)))
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "prod_ser" -> ps.get)))
    case (u, None, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, "prod_ser" -> ps.get)))
    case (u, None, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, "prod_ser" -> ps.get)))
    case (u, None, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "prod_ser" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "prod_ser" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "prod_ser" -> ps.get)))
  }
  
  def matchObjectBySubSCMS(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, None, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, subSCMSField -> ps.get)))
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, subSCMSField -> ps.get)))
    case (u, None, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, subSCMSField -> ps.get)))
    case (u, None, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, subSCMSField -> ps.get)))
    case (u, None, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, subSCMSField -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, subSCMSField -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, subSCMSField -> ps.get)))
  }

  def matchObjectBySpecialSubSCMS(user: Option[String]=None, year: Option[String]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, None, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> ps.get)))
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, "$or" -> ps.get)))
    case (u, None, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, "$or" -> ps.get)))
    case (u, None, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, "$or" -> ps.get)))
    case (u, None, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$or" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, "$or" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, "$or" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, yearField -> y.get, quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$or" -> ps.get)))
  }

  
  def matchObjectByLast3YearsPeriods(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week) match {
    case (None, None, None, None, None) => None
    case (u, y, None, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y))))
    case (u, y, q, None, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> List(MongoDBObject(yearField -> y.get._1), yearField -> y.get._2, yearField -> y.get._3)), quarterField -> q.get))
    case (u, y, q, m, None) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get)))
    case (u, y, q, m, w) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get)))
  }

  def matchObjectByLast3YearsProdSer(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), "prod_ser" -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "prod_ser" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "prod_ser" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "prod_ser" -> ps.get)))
  }

  def matchObjectByLast3YearsSubSCMS(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[String]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), subSCMSField -> ps.get)))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, subSCMSField -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, subSCMSField -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, subSCMSField -> ps.get)))
  }
  
  def matchObjectByLast3YearsSpecialSubSCMS(user: Option[String]=None, year: Option[Tuple3[String, String, String]]=None, quarter: Option[String]=None, month: Option[String]=None, week: Option[String]=None, prodSer: Option[List[DBObject]]=None): Option[DBObject] = (user, year, quarter, month, week, prodSer) match {
    case (None, None, None, None, None, None) => None
    case (u, y, None, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> List(MongoDBObject(yearField -> y.get._1), MongoDBObject(yearField -> y.get._2), MongoDBObject(yearField -> y.get._3, "$or" -> ps.get)))))
    case (u, y, q, None, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, "$or" -> ps.get)))
    case (u, y, q, m, None, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, "$or" -> ps.get)))
    case (u, y, q, m, w, ps) => 
      Some(MongoDBObject("$match" -> MongoDBObject(mappedToField -> u.get, "$or" -> last3YearsQryObj(y), quarterField -> q.get, monthField -> m.get, weekField -> w.get, "$or" -> ps.get)))
  }


  
}