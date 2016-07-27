package dataModels.truenorth

import java.util.concurrent._
import helpers.{Calculator}
import com.mongodb.casbah.Imports._
import connUtil.nosql.{MongoConnection}
import com.mongodb.casbah.WriteConcern

class Authorization(val userCursor: DBObject, val client: MongoClient) extends Runnable {
  
  def run {
    val bookColl: MongoCollection = MongoConnection().getMongoCollection(client, "booking_dump2")    
    val nodeColl: MongoCollection = MongoConnection().getMongoCollection(client, "unique_nodes_all")
    val userColl: MongoCollection = MongoConnection().getMongoCollection(client, "users")
    val userCredential: Tuple3[String, List[String], List[String]] = getUserCredentials(userCursor)
    val userName = userCredential._1; val sl6List = userCredential._2; val salesAgentsList = userCredential._3
    println("Mapping User: " +  userCredential._1)
    val startTime = System.currentTimeMillis
    MongoUpdater.activateUser(userColl, userName)
    userName match {
      case "jeydurai" | "ngollagu" | "dmalkani" | "pnithin" | "guest" | "shanagar" | "jjethana" | "rashetty" | "mronad" => {
        val qObj = MongoDBObject()
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case "mmaniman" => {
        val qObj = MongoDBObject("location_nodes.region" -> "SOUTH")
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case "timitra" => {
        val qObj = MongoDBObject("location_nodes.region" -> "WEST")
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case "vimahaja" | "nidbansa" => {
        val qObj = MongoDBObject("location_nodes.region" -> "NORTH")
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case "abhbaner" => {
        val qObj = MongoDBObject("$or" -> List(MongoDBObject("location_nodes.region" -> "EAST"), MongoDBObject("location_nodes.region" -> "SAARC")))
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case "eu3_od" | "ammalik" => {
        val qObj = MongoDBObject("$or" -> List(MongoDBObject("location_nodes.region" -> "EAST"), MongoDBObject("location_nodes.region" -> "NORTH"), MongoDBObject("location_nodes.region" -> "SAARC")))
        MongoUpdater.mapUserNameInBookingDump(bookColl, qObj, userName)
      }
      case u => mapUsername(sl6List, salesAgentsList, userName, (bookColl, nodeColl))
    }
    val endTime = System.currentTimeMillis
    val timeDiff: Long  = endTime-startTime
    val secs: Long = timeDiff/1000
    val mins: Long = secs/60
    val hrs: Long = mins/60
    println("Total Process Time of " + userName + " is " + hrs.toInt.toString + " Hr(s) " + mins.toInt.toString + " Min(s) " + secs.toInt.toString + " secs(s)")
  }
  
  private def mapUsername(salesL6List: List[String], salesAgentList: List[String], uName: String, collTuple: Tuple2[MongoCollection, MongoCollection]): Unit = {
    def mapName(saList: List[String]): Boolean = saList match {
      case Nil => true
      case hd::tl => {
        updateInBookingDump(uName, hd, collTuple._1, fetchSL6BySalesAgent(hd, collTuple._2), salesL6List)
        mapName(tl)
      }
    }
    mapName(salesAgentList)
  }
  
  private def updateInBookingDump(userName: String, salesAgent: String, coll: MongoCollection, baseSL6List: List[String], targetSL6List: List[String]): Unit = {
    def update(list: List[String]): Boolean = list match {
      case Nil => true
      case hd::tl if targetSL6List contains hd => {
        MongoUpdater.mapUserNameInBookingDump(
            coll, 
            MongoDBObject("names.sales_agent.name" -> salesAgent, "location_nodes.sales_level_6" -> hd), 
            userName
        ) //mapUserNameInBookingDump
        update(tl)
      }
      case hd::tl => update(tl)
    }
    update(baseSL6List)
  }
  
  private def fetchSL6BySalesAgent(sa: String, coll: MongoCollection): List[String] = {
    val aggQry: List[DBObject] = List(
        MongoDBObject("$match" -> MongoDBObject("sales_agents" -> sa)),
        MongoDBObject("$group" -> MongoDBObject("_id" -> "$sales_level_6", "recs" -> MongoDBObject("$sum" -> 1)))
    ) // List
    val aggOptions = AggregationOptions(AggregationOptions.CURSOR)
    val docs = coll.aggregate(aggQry, aggOptions)
    val cursor: Iterator[DBObject] = for (x <- docs) yield x
    val sl6List: List[String] = cursor match {
      case c if c.isEmpty => List[String]()
      case c => {
        val list = scala.collection.mutable.Buffer[String]()
        c.foreach { x =>
          val sl6: String = x.getAs[String]("_id") match {
            case None => ""
            case Some(v) => v
          } // match
          list += sl6
        } //foreach
        list.toList
      } // case c
    } // cursor match
    sl6List
  }
  
  
  
  def getUserCredentials(cursor: DBObject): Tuple3[String, List[String], List[String]] = cursor match {
      case c if c.isEmpty => ("", List(), List())
      case c => {
        val user: String = c.getAs[String]("username") match {
          case None => ""
          case Some(n) => n
        }
        val sl6: List[String] = c.getAs[DBObject]("accessibility") match {
          case None => List[String]()
          case Some(v) => {
            val locObj: DBObject = v.getAs[DBObject]("location") match {
              case None => MongoDBObject()
              case Some(v2) => v2
            }
            val list: List[String] = locObj.getAs[List[String]]("sales_level_6") match {
              case None => List[String]()
              case Some(v3) => v3.toList
            }
            list
          }
        }
        val salesAgents: List[String] = c.getAs[DBObject]("accessibility") match {
          case None => List[String]()
          case Some(v) => {
            val locObj: DBObject = v.getAs[DBObject]("location") match {
              case None => MongoDBObject()
              case Some(v2) => v2
            }
            val list: List[String] = locObj.getAs[List[String]]("sales_agents") match {
              case None => List[String]()
              case Some(v3) => v3.toList
            }
            list
          }
        }
        (user, sl6, salesAgents)
      }
      
  }
  
}