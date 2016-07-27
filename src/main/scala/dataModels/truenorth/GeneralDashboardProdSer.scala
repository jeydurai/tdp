package dataModels.truenorth

import java.util.concurrent._
import helpers.{Calculator}
import com.mongodb.casbah.Imports._
import connUtil.nosql.{MongoConnection}
import com.mongodb.casbah.WriteConcern


class GeneralDashboardProdSer(val user: DBObject, val client: MongoClient) extends Runnable{
  def run {
    val bookingColl: MongoCollection = MongoConnection().getMongoCollection(client, "booking_dump2")    
    val gdColl: MongoCollection = MongoConnection().getMongoCollection(client, "general_dashboard_prodser")    
    gdColl.drop
    val allPeriods: Tuple2[List[Map[String, String]], Map[String, scala.collection.mutable.Set[String]]] = MongoQueries.getAllPeriods(client)
    val prodSerList = List("product", "service")
    val periodsBuffer: List[Map[String, String]] = allPeriods._1
    var dataProcessConstraints = scala.collection.mutable.Set[String]()
    val startTime = System.currentTimeMillis
    for (prodSer <- prodSerList) {
      for (periods <- periodsBuffer /*if periods("year").toInt >= 2015*/) {
        val params = (user("username").toString, periods("year"), periods("quarter"), periods("month"), periods("week"), prodSer, bookingColl)
        val constraints1: String = prodSer + "|" + user("username").toString + "|" + periods("year")
        val constraints2: String = prodSer + "|" + user("username").toString + "|" + periods("year") + "|" + periods("quarter")
        val constraints3: String = prodSer + "|" + user("username").toString + "|" + periods("year") + "|" + periods("quarter") + "|" + periods("month")
        val constraints4: String = prodSer + "|" + user("username").toString + "|" + periods("year") + "|" + periods("quarter") + "|" + periods("month") + "|" + periods("week")
        val constraintsSet = dataProcessConstraints.toSet
        println(prodSer + " | " + user("username") + " Year: " + periods("year") + " Quarter: " + periods("quarter") + " Month: " + periods("month") + 
            " Week: " + periods("week")
        )
        if (!constraintsSet.contains(constraints1)) {
          //println("till date initiated in constraints1!")
          writeGeneralDashboard(gdColl, processData((user("username").toString, Some(periods("year")), None, None, None, Some(prodSer), bookingColl)))
        }
        if (!constraintsSet.contains(constraints2)) {
          //println("till date initiated in constraints2!")
          writeGeneralDashboard(gdColl, processData((user("username").toString, 
                Some(periods("year")), Some(periods("quarter")), None, None, Some(prodSer), bookingColl)))
        }
        if (!constraintsSet.contains(constraints3)) {
          //println("till date initiated in constraints3!")
          writeGeneralDashboard(gdColl, processData((user("username").toString, 
                Some(periods("year")), Some(periods("quarter")), Some(periods("month")), None, Some(prodSer), bookingColl)))
        }
        if (!constraintsSet.contains(constraints4)) {
          //println("till date initiated in constraints4!")
          writeGeneralDashboard(gdColl, processData((user("username").toString, 
                Some(periods("year")), Some(periods("quarter")), Some(periods("month")), Some(periods("week")), Some(prodSer), bookingColl)))
        }
        dataProcessConstraints += constraints1; dataProcessConstraints += constraints2; dataProcessConstraints += constraints3; dataProcessConstraints += constraints4
      }
    }
    val endTime = System.currentTimeMillis
    val timeDiff: Long  = endTime-startTime
    val secs: Long = timeDiff/1000
    val mins: Long = secs/60
    val hrs: Long = mins/60
    println("Total Process Time of " + user("username") + " is " + hrs.toInt.toString + " Hr(s) " + mins.toInt.toString + " Min(s) " + secs.toInt.toString + " secs(s)")
  }

  private def processData(params: Tuple7[String, Option[String], Option[String], Option[String], Option[String], Option[String], MongoCollection]): MongoDBObject = {
      val mongoObj = getMongoMatchObj(params)
      val matchObj = mongoObj._1; val matchObjForYoY = mongoObj._2; val matchObjForQoQ =  mongoObj._3; val matchObjForMoM =  mongoObj._4; val matchObjForWoW =  mongoObj._5
      val matchObjCusPrevYear =  mongoObj._6; val matchObjCusMorePrevYear =  mongoObj._7; val matchObjCus =  mongoObj._8; val matchObjCusSO =  mongoObj._9
      val matchObjPar =  mongoObj._10

      val year = params._2.getOrElse(None); val quarter = params._3.getOrElse(None); val month = params._4.getOrElse(None); val week = params._5.getOrElse(None); val prodSer = params._6.getOrElse(None)
      val coll = params._7

      val mongoPeriods = MongoDBObject("year" -> year, "quarter" -> quarter, "month" -> month, "week" -> month, "prod_ser" -> prodSer)
      val tillDateBooking = getBookingBySplits(matchObj, coll, "TILL_DATE")
      val mongoTDBooking = MongoDBObject("xAxis" -> tillDateBooking._1, "yAxis" -> tillDateBooking._2)
      val arch2Booking = getBookingBySplits(matchObj, coll, "ARCH2")
      val mongoArch2Booking = MongoDBObject("xAxis" -> arch2Booking._1, "yAxis" -> arch2Booking._2)
      val verticalBooking = getBookingBySplits(matchObj, coll, "VERTICAL")
      val mongoVerticalBooking = MongoDBObject("xAxis" -> verticalBooking._1, "yAxis" -> verticalBooking._2)
      val techBooking = getBookingBySplits(matchObj, coll, "TECH_NAME")
      val mongoTechNameBooking = MongoDBObject("xAxis" -> techBooking._1, "yAxis" -> techBooking._2)
      val atAttachBooking = getBookingBySplits(matchObj, coll, "AT_ATTACH")
      val mongoATAttachBooking = MongoDBObject("xAxis" -> atAttachBooking._1, "yAxis" -> atAttachBooking._2)
      val subSCMSBooking = getBookingBySplits(matchObj, coll, "SUB_SCMS")
      val mongoSubSCMSBooking = MongoDBObject("xAxis" -> subSCMSBooking._1, "yAxis" -> subSCMSBooking._2)
      val gtmuBooking = getBookingBySplits(matchObj, coll, "GTMU")
      val mongoGTMuBooking = MongoDBObject("xAxis" -> gtmuBooking._1, "yAxis" -> gtmuBooking._2)
      val regionBooking = getBookingBySplits(matchObj, coll, "REGION")
      val mongoRegionBooking = MongoDBObject("xAxis" -> regionBooking._1, "yAxis" -> regionBooking._2)
      val sl6Booking = getBookingBySplits(matchObj, coll, "SL6")
      val mongoSL6Booking = MongoDBObject("xAxis" -> sl6Booking._1, "yAxis" -> sl6Booking._2)
      val topCusBooking = getBookingBySplits(matchObjCus, coll, "TOP_CUSTOMERS")
      val mongoTopCusBooking = MongoDBObject("xAxis" -> topCusBooking._1, "yAxis" -> topCusBooking._2)
      val topParBooking = getBookingBySplits(matchObjPar, coll, "TOP_PARTNERS")
      val mongoTopParBooking = MongoDBObject("xAxis" -> topParBooking._1, "yAxis" -> topParBooking._2)
      val topCusDealsBooking = getBookingBySplits(matchObjCusSO, coll, "TOP_CUS_DEALS")
      val mongoTopCusDealsBooking = MongoDBObject("xAxis" -> topCusDealsBooking._1, "yAxis" -> topCusDealsBooking._2)
      val billedCustomers = getBookingBySplits(matchObjCus, coll, "BILLED_CUSTOMERS"); 
      val mongobilledCusBooking = MongoDBObject("xAxis" -> billedCustomers._1, "yAxis" -> billedCustomers._2)
      val billedParBooking = getBookingBySplits(matchObjPar, coll, "BILLED_PARTNERS")
      val mongoBilledParBooking = MongoDBObject("xAxis" -> billedParBooking._1, "yAxis" -> billedParBooking._2)
      val qoqBooking: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObjForQoQ, coll, "QoQ_BOOKING")
      val mongoQoQBooking = MongoDBObject("xAxis" -> qoqBooking._1, "yAxis" -> qoqBooking._2)
      val momBooking: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObjForMoM, coll, "MoM_BOOKING")
      val mongoMoMBooking = MongoDBObject("xAxis" -> momBooking._1, "yAxis" -> momBooking._2)
      val wowBooking: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObjForWoW, coll, "WoW_BOOKING")
      val mongoWoWBooking = MongoDBObject("xAxis" -> wowBooking._1, "yAxis" -> wowBooking._2)
      val prodSerBooking: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObj, coll, "PRODSER_BOOKING")
      val mongoProdSerBooking = MongoDBObject("xAxis" -> prodSerBooking._1, "yAxis" -> prodSerBooking._2)
      val discountAll: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObj, coll, "DISCOUNT_ALL")
      val mongoDisAllBooking = MongoDBObject("xAxis" -> discountAll._1, "yAxis" -> discountAll._2)
      val discountArchs: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObj, coll, "DISCOUNT_ARCHS")
      val mongoDisArchsBooking = MongoDBObject("xAxis" -> discountArchs._1, "yAxis" -> discountArchs._2)
      val bookingHistory: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObjForYoY, coll, "BOOKING_HISTORY")
      val mongoBookingHistory = MongoDBObject("xAxis" -> bookingHistory._1, "yAxis" -> bookingHistory._2)

      val technologyPenetration: Tuple2[List[String], List[Double]] = getBookingBySplits(matchObjCus, coll, "TECH_PENETRATION")
      val totalTechs = technologyPenetration._1.length; val totalUniqueCus = technologyPenetration._1.distinct
      val techPen = Calculator.calculateRatio((totalTechs, totalUniqueCus.length))
      val mongoTechPen = MongoDBObject("xAxis" ->List("techPenetration"), "yAxis" -> List(techPen))
      
      val yldPerCusParams: Tuple2[Double, Int] = (tillDateBooking._2, billedCustomers._2) match {
        case (Nil, Nil) => (0d, 0)
        case (List(), List()) => (0d, 0)
        case (List(b), List(c)) => (b, c.toInt)
        case (_, _) => (0d, 0)
      }
      
      val yldPerCus = Calculator.calculateRatio((yldPerCusParams._1, yldPerCusParams._2))/Calculator.THOUSAND
      val mongoYLDPerCus = MongoDBObject("xAxis" -> List("customer"), "yAxis" -> List(yldPerCus))

      val mongoNewRepeatDormant = getMongoNewRepeatDormant((matchObjCus, matchObjCusPrevYear, matchObjCusMorePrevYear), (params._3, params._4, params._5), coll)      
      
      val mongoObject = MongoDBObject(
          "username" -> params._1,
          "periods" -> mongoPeriods,
          "tdBooking" -> mongoTDBooking,
          "archBooking" -> mongoArch2Booking,
          "verticalBooking" -> mongoVerticalBooking,
          "techBooking" -> mongoTechNameBooking,
          "atAttachBooking" -> mongoATAttachBooking,
          "subSCMSBooking" -> mongoSubSCMSBooking,
          "gtmuBooking" -> mongoGTMuBooking,
          "regionBooking" -> mongoRegionBooking,
          "topCustomerBooking" -> mongoTopCusBooking,
          "topPartnerBooking" -> mongoTopParBooking,
          "topSL6Booking" -> mongoSL6Booking,
          "topDeals" -> mongoTopCusDealsBooking,
          "billedCustomers" -> mongobilledCusBooking,
          "newAccounts" -> mongoNewRepeatDormant._1,
          "repeatAccounts" -> mongoNewRepeatDormant._2,
          "dormantAccounts" -> mongoNewRepeatDormant._3,
          "billedPartners" -> mongoBilledParBooking,
          "techPenetration" -> mongoTechPen,
          "qoqBooking" -> mongoQoQBooking,
          "momBooking" -> mongoMoMBooking,
          "wowBooking" -> mongoWoWBooking,
          "prodSerBooking" -> mongoProdSerBooking,
          "disArchsBooking" -> mongoDisArchsBooking,
          "disAllBooking" -> mongoDisAllBooking,
          "bookingHistory" -> mongoBookingHistory,
          "yieldPerCustomer" -> mongoYLDPerCus
      )
      mongoObject
  }

  private def writeGeneralDashboard(coll: MongoCollection, obj: MongoDBObject) =  {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.insert(obj, wc)
  }
  
  private def getMongoNewRepeatDormant(mongoObj: Tuple3[DBObject, DBObject, DBObject], peridCons: Tuple3[Option[String], Option[String], Option[String]], coll: MongoCollection): Tuple3[MongoDBObject, MongoDBObject, MongoDBObject] = {
      val mongoNewRepeatDormant: Tuple3[MongoDBObject, MongoDBObject, MongoDBObject] = (peridCons._1.getOrElse(None), peridCons._2.getOrElse(None), peridCons._3.getOrElse(None)) match {
        case (None, None, None) => {
          val customersThisYear: Tuple2[List[String], List[Double]] = getBookingBySplits(mongoObj._1, coll, "CUSTOMERS_THIS_YEAR")
          val customersLastYear: Tuple2[List[String], List[Double]] = getBookingBySplits(mongoObj._2, coll, "CUSTOMERS_LAST_YEAR")
          val customersLast3Years: Tuple2[List[String], List[Double]] = getBookingBySplits(mongoObj._3, coll, "CUSTOMERS_LAST_3YEARS")
          val funSubsetNewRepeat = reduceTwoDataset(customersThisYear) _
          val funSubsetDormant = reduceTwoDataset2(customersThisYear) _
          
          val newRepeatLastYear: Tuple7[String, Int, Double, Int, Double, Int, Double] = funSubsetNewRepeat(customersLastYear, "NR_LY")
          val newRepeatMoreLastYear: Tuple7[String, Int, Double, Int, Double, Int, Double] = funSubsetNewRepeat(customersLast3Years, "NR_MLY")
          val dormantLastYear: Tuple7[String, Int, Double, Int, Double, Int, Double] = funSubsetDormant(customersLastYear, "D_LY")
          val dormantMoreLastYear: Tuple7[String, Int, Double, Int, Double, Int, Double] = funSubsetDormant(customersLast3Years, "D_MLY")
          val mongoNewAccounts = MongoDBObject(
                  "xAxis" -> newRepeatLastYear._1,
                  "yAxis0" -> newRepeatLastYear._2,
                  "yAxis" -> newRepeatLastYear._3,
                  "yAxis2" -> newRepeatLastYear._4,
                  "yAxis3" -> newRepeatLastYear._5,
                  "yAxis4" -> newRepeatMoreLastYear._4,
                  "yAxis5" -> newRepeatMoreLastYear._5
          )
          val mongoRepeatAccounts = MongoDBObject(
                  "xAxis" -> newRepeatMoreLastYear._1,
                  "yAxis0" -> newRepeatMoreLastYear._2,
                  "yAxis" -> newRepeatMoreLastYear._3,
                  "yAxis2" -> newRepeatMoreLastYear._6,
                  "yAxis3" -> newRepeatMoreLastYear._7,
                  "yAxis4" -> newRepeatLastYear._6,
                  "yAxis5" -> newRepeatLastYear._7
          )
          val mongoDormantAccounts = MongoDBObject(
                  "xAxis" -> dormantLastYear._1,
                  "yAxis0" -> dormantLastYear._2,
                  "yAxis" -> dormantLastYear._3,
                  "yAxis2" -> dormantLastYear._4,
                  "yAxis3" -> dormantLastYear._5,
                  "yAxis4" -> dormantMoreLastYear._2,
                  "yAxis5" -> dormantMoreLastYear._4,
                  "yAxis6" -> dormantMoreLastYear._5
          )
          (mongoNewAccounts, mongoRepeatAccounts, mongoDormantAccounts)
        }
        case _ => {
          val mongoNewAccounts = MongoDBObject("xAxis" -> Nil, "yAxis0" -> Nil, "yAxis" -> Nil, "yAxis2" -> Nil, "yAxis3" -> Nil, "yAxis4" -> Nil, "yAxis5" -> Nil)
          val mongoRepeatAccounts = MongoDBObject("xAxis" -> Nil, "yAxis0" -> Nil, "yAxis" -> Nil, "yAxis2" -> Nil, "yAxis3" -> Nil, "yAxis4" -> Nil, "yAxis5" -> Nil)
          val mongoDormantAccounts = MongoDBObject("xAxis" -> Nil, "yAxis0" -> Nil, "yAxis" -> Nil, "yAxis2" -> Nil, "yAxis3" -> Nil, "yAxis4" -> Nil, "yAxis5" -> Nil, "yAxis6" -> Nil)
          (mongoNewAccounts, mongoRepeatAccounts, mongoDormantAccounts)
        }
      }
      mongoNewRepeatDormant
  }
  
  private def reduceTwoDataset(cusThisYear: Tuple2[List[String], List[Double]]) (cusLastYear: Tuple2[List[String], List[Double]], label: String): 
  Tuple7[String, Int, Double, Int, Double, Int, Double] = {
    val inputTuple: Tuple2[Tuple2[List[String], List[Double]], Tuple2[List[String], List[Double]]] = (cusThisYear, cusLastYear)
    val result = inputTuple match {
      case (thisYear, lastYear) => {
        val cCusList = thisYear._1; val pCusList = lastYear._1; val cBookingList = thisYear._2; val pBookingList = lastYear._2
        val cCusSet = cCusList.toSet; val pCusSet = pCusList.toSet
        val cCusMap = (cCusList zip cBookingList).toMap
        val newToPrevList = cCusSet.diff(pCusSet).toList; val repeatToPrevList = cCusSet.intersect(pCusSet).toList
        //println(newToPrevList.toString)
        val funSumBooking = sumCusBookingMap(cCusMap)_
        val newToPrevAccounts = newToPrevList.length; val newToPrevBooking = funSumBooking(newToPrevList)
        val repeatToPrevAccounts = repeatToPrevList.length; val repeatToPrevBooking = funSumBooking(repeatToPrevList)
        val cBookingSum = cBookingList.sum
        //println("cBookingList Sum: " + cBookingSum.toString + " | " + "repeatToPrevBooking: " + repeatToPrevBooking.toString)
        (label, cCusList.length, cBookingSum, newToPrevAccounts, newToPrevBooking, repeatToPrevAccounts, repeatToPrevBooking)
      }
      case _ => (label, 0, 0d, 0, 0d, 0, 0d)
    }
    result
  }

  private def reduceTwoDataset2(cusThisYear: Tuple2[List[String], List[Double]]) (cusLastYear: Tuple2[List[String], List[Double]], label: String): 
  Tuple7[String, Int, Double, Int, Double, Int, Double] = {
    val inputTuple: Tuple2[Tuple2[List[String], List[Double]], Tuple2[List[String], List[Double]]] = (cusLastYear, cusThisYear) // lastYear and thisYear val identifiers have been switched to satisfy the algorithm for dormant accounts 
    val result = inputTuple match {
      case (thisYear, lastYear) => { 
        val cCusList = thisYear._1; val pCusList = lastYear._1; val cBookingList = thisYear._2; val pBookingList = lastYear._2
        val cCusSet = cCusList.toSet; val pCusSet = pCusList.toSet
        val cCusMap = (cCusList zip cBookingList).toMap
        val newToPrevList = cCusSet.diff(pCusSet).toList; val repeatToPrevList = cCusSet.intersect(pCusSet).toList
        val funSumBooking = sumCusBookingMap(cCusMap)_
        val newToPrevAccounts = newToPrevList.length; val newToPrevBooking = funSumBooking(newToPrevList)
        val repeatToPrevAccounts = repeatToPrevList.length; val repeatToPrevBooking = funSumBooking(repeatToPrevList)
        (label, cCusList.length, cBookingList.sum, newToPrevAccounts, newToPrevBooking, repeatToPrevAccounts, repeatToPrevBooking)
      }
      case _ => (label, 0, 0d, 0, 0d, 0, 0d)
    }
    result
  }
  
  
  private def sumCusBookingMap(cusMap: Map[String, Double])(cusList: List[String]): Double = {
    def sumBooking(list: List[String], accum: Double): Double = list match {
      case Nil => accum
      case head::tail => sumBooking(tail, accum+cusMap(head))
    }
    sumBooking(cusList, 0)
  }
  
  // All General Dashboard Supporting Methods
  // ****************************************
  // ========================================
  
  private def getMongoMatchObj(params: Tuple7[String, Option[String], Option[String], Option[String], Option[String],Option[String], MongoCollection]): Tuple10[DBObject, DBObject, DBObject, DBObject, DBObject, DBObject, DBObject, DBObject, DBObject, DBObject] = {
    val user: String = params._1; 
    val year = params._2
    val yearAsInt = year.getOrElse("").toInt
    val quarter = params._3
    val month = params._4
    val week = params._5
    val ps = params._6

    val prevYear1 = (yearAsInt-1).toString; val prevYear2 = (yearAsInt-2).toString; val prevYear3 = (yearAsInt-3).toString
    
    val mongoObj = ps match {
      case v  => {
        val common = MongoQueryObjects.matchObjectByProdSer(Some(user), year, quarter, month, week, ps).getOrElse(MongoDBObject())
        val yoy = MongoQueryObjects.matchObjectByProdSer(Some(user), None, quarter, month, week, ps).getOrElse(MongoDBObject())
        val qoq = MongoQueryObjects.matchObjectByProdSer(Some(user), year, None, None, None, ps).getOrElse(MongoDBObject())
        val mom = MongoQueryObjects.matchObjectByProdSer(Some(user), year, quarter, None, None, ps).getOrElse(MongoDBObject())
        val wow = MongoQueryObjects.matchObjectByProdSer(Some(user), year, quarter, month, None, ps).getOrElse(MongoDBObject())
        val cusprevYear = MongoQueryObjects.matchObjectByProdSer(Some(user), Some(prevYear1), quarter, month, week, ps).getOrElse(MongoDBObject())
        val cusMorePrevYear = MongoQueryObjects.matchObjByProdSerCustomerNotLast3Years(Some(user), Some((prevYear1, prevYear2, prevYear3)), quarter, month, week, ps).getOrElse(MongoDBObject())
        val cus = MongoQueryObjects.matchObjByProdSerCustomerNot(Some(user), year, quarter, month, week, ps).getOrElse(MongoDBObject())
        val cusSO = MongoQueryObjects.matchObjByProdSerCustomerDealsNot(Some(user), year, quarter, month, week, ps).getOrElse(MongoDBObject())
        val par = MongoQueryObjects.matchObjByProdSerPartnerNot(Some(user), year, quarter, month, week, ps).getOrElse(MongoDBObject())
        (common, yoy, qoq, mom, wow, cusprevYear, cusMorePrevYear, cus, cusSO, par)
      }
      
    }
    mongoObj
  }
  
  
  private def getBookingBySplits(matchObj: DBObject, coll: MongoCollection, dataKind: String): Tuple2[List[String], List[Double]] = {
    
    val howToQry: Tuple2[List[DBObject], Tuple2[Int, String]] = dataKind match {
      case "TILL_DATE" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByHistory,
          MongoDBObject("$sort" -> MongoDBObject("_id" -> -1))
        ), (1, ""))
      }
      case "ARCH2" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByArch2,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "VERTICAL" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByVertical,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "TECH_NAME" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByTechName,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "AT_ATTACH" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByAtAttach,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "SUB_SCMS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingBySubSCMS,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "GTMU" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByGTMu,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "REGION" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByRegion,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1))
        ), (1, ""))
      }
      case "SL6" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingBySL6,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1)),
          MongoDBObject("$limit" -> 10)
        ), (1, ""))
      }
      case "TOP_CUSTOMERS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomer,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1)),
          MongoDBObject("$limit" -> 10)
        ), (1, ""))
      }
      case "TOP_PARTNERS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByPartner,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1)),
          MongoDBObject("$limit" -> 10)
        ), (1, ""))
      }
      case "TOP_CUS_DEALS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomerSONumber,
          MongoDBObject("$sort" -> MongoDBObject("booking" -> -1)),
          MongoDBObject("$limit" -> 10)
        ), (2, ""))
      }
      case "BILLED_CUSTOMERS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupByCustomers
        ), (3, "billedCustomers"))
      }
      case "BILLED_PARTNERS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupByPartners
        ), (3, "billedPartners"))
      }
      case "CUSTOMERS_THIS_YEAR" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomer()
        ), (1, ""))
      }
      case "CUSTOMERS_LAST_YEAR" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomer()
        ), (1, ""))
      }
      case "CUSTOMERS_LAST_3YEARS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomer()
        ), (1, ""))
      }
      case "TECH_PENETRATION" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByCustomerTechologies
        ), (1, ""))
      }
      case "QoQ_BOOKING" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByQoQ
        ), (1, ""))
      }
      case "MoM_BOOKING" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByMoM
        ), (1, ""))
      }
      case "WoW_BOOKING" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByWoW
        ), (1, ""))
      }
      case "PRODSER_BOOKING" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByProductService
        ), (1, ""))
      }
      case "DISCOUNT_ALL" => {
        (List(
          matchObj,
          MongoQueryObjects.groupExclusiveBookingNetAndList
        ), (4, ""))
      }
      case "DISCOUNT_ARCHS" => {
        (List(
          matchObj,
          MongoQueryObjects.groupNetAndListByArchs
        ), (4, ""))
      }
      case "BOOKING_HISTORY" => {
        (List(
          matchObj,
          MongoQueryObjects.groupBookingByHistory
        ), (1, ""))
      }
    }
    
    MongoQueries.aggregateQuery(coll, howToQry._1, howToQry._2)
  }
  
  
}