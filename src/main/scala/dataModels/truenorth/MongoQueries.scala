package dataModels.truenorth

import connUtil.nosql.{MongoConnection}
import com.mongodb.casbah.Imports._
import helpers.{Calculator}
import com.mongodb.casbah.WriteConcern

object MongoQueries {
  
  def getAllApprovedUsers(): Iterator[DBObject] = {
    val coll = MongoConnection().getTrueNorthUsers()
    val q = MongoDBObject("approval_status.code" -> 1)
    val fields = MongoDBObject("username" -> 1)
    val users = for (x <- coll.find(q, fields)) yield x
    users
  }

  def fetchAllApprovedUsers(): Iterator[DBObject] = {
    val coll = MongoConnection().getTrueNorthUsers()
    val q = MongoDBObject("approval_status.code" -> 2)
    //val q = MongoDBObject("approval_status.code" -> 1)
    val fields = MongoDBObject("username" -> 1, "accessibility.location.sales_level_6" -> 1, "accessibility.location.sales_agents" -> 1)
    val users = for (x <- coll.find(q, fields)) yield x
    users
  }
  
  def getAllPeriods(client: MongoClient): Tuple2[List[Map[String, String]], Map[String, scala.collection.mutable.Set[String]]] = {
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    val aggQry = List(
        MongoQueryObjects.groupByPeriods,
        MongoDBObject("$sort" -> MongoDBObject("_id.fiscal_year" -> -1, "_id.fiscal_quarter" -> -1, "_id.fiscal_month" -> -1, "_id.fiscal_week" -> -1))
    )
    val db = client("truenorth")
    val coll: MongoCollection = db("booking_dump2")
    val docs = coll.aggregate(
        aggQry, aggOptions
    )
    val years = scala.collection.mutable.Set[String]()
    val quarters = scala.collection.mutable.Set[String]()
    val months = scala.collection.mutable.Set[String]()
    val weeks = scala.collection.mutable.Set[String]()
    val periodMutBuffer = scala.collection.mutable.Buffer[Map[String, String]]() 
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    
    for (doc <- cursor) {
      val subDoc = doc.getAs[DBObject]("_id").get
      years += subDoc.getAs[String]("fiscal_year").get
      quarters += subDoc.getAs[String]("fiscal_quarter").get
      months += subDoc.getAs[String]("fiscal_month").get
      weeks += subDoc.getAs[String]("fiscal_week").get
      val periodMap = Map (
          "year" -> subDoc.getAs[String]("fiscal_year").get,
          "quarter" -> subDoc.getAs[String]("fiscal_quarter").get,
          "month" -> subDoc.getAs[String]("fiscal_month").get,
          "week" -> subDoc.getAs[String]("fiscal_week").get
      )
      periodMutBuffer += periodMap
    }
    val periodBuffer = periodMutBuffer.toList
    val periodCredentials = Map(
        "yMap" -> years,
        "qMap" -> quarters,
        "mMap" -> months,
        "wMap" -> weeks
    )
    val returnData:Tuple2[List[Map[String, String]], Map[String, scala.collection.mutable.Set[String]]] = (periodBuffer, periodCredentials)
    returnData
  }
  
  def aggregateQuery(coll: MongoCollection, aggQry: List[DBObject], qryConfig: Tuple2[Int, String]=(1, "")): Tuple2[List[String], List[Double]] = {
    val result: Tuple2[List[String], List[Double]] = qryConfig match {
      case (1, "") => keyValuePairByAggQry(coll, aggQry)
      case (2, "") => objKeyValuePairByAggQry(coll, aggQry)
      case (3, x) => uniqueValueByAggQry(coll, aggQry, x)
      case (4, "") => keyValuePairDiscountByAggQry(coll, aggQry)
    }
    result
  }

  private def keyValuePairByAggQry(coll: MongoCollection, aggQry: List[DBObject]): Tuple2[List[String], List[Double]] = {
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    //println(matchObj)
    val docs = coll.aggregate(aggQry, aggOptions)
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    
    val result: Tuple2[List[String], List[Double]] = cursor match {
      case c if c.isEmpty => (Nil, Nil)
      case c => {
        val keyBuffer = scala.collection.mutable.Buffer[String]()
        val valueBuffer = scala.collection.mutable.Buffer[Double]()
        
        c.foreach { x =>  
          val key = x.getAs[String]("_id") match {
            case None => ""
            case Some(v) => v
          }
          val value = x.getAs[Double]("booking") match {
            case None => 0d
            case Some(v) => v
          }
          keyBuffer += key
          valueBuffer += value/Calculator.MILLION
          //println(key + " | " + value.toString)
        }
        (keyBuffer.toList, valueBuffer.toList)
      }
    }
    result
  }

  private def keyValuePairDiscountByAggQry(coll: MongoCollection, aggQry: List[DBObject]): Tuple2[List[String], List[Double]] = {
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    //println(matchObj)
    val docs = coll.aggregate(aggQry, aggOptions)
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    
    val result: Tuple2[List[String], List[Double]] = cursor match {
      case c if c.isEmpty => (Nil, Nil)
      case c => {
        val keyBuffer = scala.collection.mutable.Buffer[String]()
        val valueBuffer = scala.collection.mutable.Buffer[Double]()
        
        c.foreach { x =>  
          val key = x.getAs[String]("_id") match {
            case None => ""
            case Some(v) => v
          }
          val value = x.getAs[Double]("booking") match {
            case None => 0d
            case Some(v) => v
          }
          val value2 = x.getAs[Double]("base_list") match {
            case None => 0d
            case Some(v) => v
          }
          keyBuffer += key
          valueBuffer += Calculator.calculateDiscount((value, value2)) 
          //println(key + " | " + value.toString)
        }
        (keyBuffer.toList, valueBuffer.toList)
      }
    }
    result
  }
  
  
  private def uniqueValueByAggQry(coll: MongoCollection, aggQry: List[DBObject], byName: String): Tuple2[List[String], List[Double]] = {
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    val docs = coll.aggregate(aggQry, aggOptions)
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    
    val result: List[String] = cursor match {
      case c if c.isEmpty => Nil
      case c => {
        val valueBuffer = scala.collection.mutable.Buffer[String]()
        
        c.foreach { x =>  
          val value = x.getAs[String]("_id") match {
            case None => ""
            case Some(v) => v
          }
          valueBuffer += value
        }
        valueBuffer.toList
      }
    }
    (List(byName),List(result.length)) 
  }
  
  
  
  private def objKeyValuePairByAggQry(coll: MongoCollection, aggQry: List[DBObject]): Tuple2[List[String], List[Double]] = {
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    //println(matchObj)
    val docs = coll.aggregate(aggQry, aggOptions)
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    
    val result: Tuple2[List[String], List[Double]] = cursor match {
      case c if c.isEmpty => (Nil, Nil)
      case c => {
        val keyBuffer = scala.collection.mutable.Buffer[String]()
        val valueBuffer = scala.collection.mutable.Buffer[Double]()
        
        c.foreach { x =>  
          val key = x.getAs[DBObject]("_id") match {
            case Some(v) => {
              val cusName = v.getAs[String]("customers") match {
                case None => ""
                case Some(v) => v
              }
              val soNumber = v.getAs[String]("soNumbers") match {
                case None => ""
                case Some(v) => v
              }
              cusName + " (" + soNumber + ")"
            }
            case None => ""
          }
          val value = x.getAs[Double]("booking") match {
            case None => 0d
            case Some(v) => v
          }
          keyBuffer += key
          valueBuffer += value/Calculator.MILLION
          //println(key + " | " + value.toString)
        }
        (keyBuffer.toList, valueBuffer.toList)
      }
    }
    result
  }
   
}