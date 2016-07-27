package dataModels.truenorth

import connUtil.nosql.{MongoConnection}
import com.mongodb.casbah.Imports._
import helpers.{Calculator}
import com.mongodb.casbah.WriteConcern


object MongoUpdater {

  def setUsersApproved(coll: MongoCollection): Boolean = {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.update(
        MongoDBObject("approval_status.code" -> 1), 
        MongoDBObject("$set" -> MongoDBObject("approval_status.code" -> 2, "approval_status.description" -> "APPROVED")), 
        false, 
        true, 
        wc
    ) //update
    true
  }

  def removeMappedToField(coll: MongoCollection): Boolean = {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.update(
        MongoDBObject(), 
        MongoDBObject("$unset" -> MongoDBObject("mappedTo" -> "")), 
        false, 
        true, 
        wc
    ) //update
    true
  }

  def mapUserNameInBookingDump(coll: MongoCollection, qObj: DBObject, userName: String): Unit = {
    val wc: WriteConcern = WriteConcern.Normal
    coll.update(
        qObj, 
        MongoDBObject("$addToSet" -> MongoDBObject("mappedTo" -> userName)), 
        false, 
        true, 
        wc
    ) //update
  }
  
  def activateUser(coll: MongoCollection, user: String): Unit = {
    val wc: WriteConcern = WriteConcern.Normal    
    coll.update(
        MongoDBObject("username" -> user), 
        MongoDBObject("$set" -> MongoDBObject("approval_status.code" -> 1, "approval_status.description" -> "ACTIVATED")), 
        false, 
        true, 
        wc
    ) //update
  }
  
}