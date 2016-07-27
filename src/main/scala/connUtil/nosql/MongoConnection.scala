package connUtil.nosql

import com.mongodb.casbah.Imports._

object MongoConnection {
  private val MONGO_HOST: String = "localhost"
  private val MONGO_PORT: Int = 27017
  def apply() = new MongoConnection
}

class MongoConnection {
  private val props = Map(
      "host" -> MongoConnection.MONGO_HOST,
      "port" -> MongoConnection.MONGO_PORT
  )

  def getMongoClient(): MongoClient = {
    val mongoClient = MongoClient("localhost", 27017)
    mongoClient
  }
  
  def getTrueNorthBookingDump(): MongoCollection = {
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("truenorth")
    val coll = db("booking_dump")
    coll
  }
  
  def getTrueNorthUsers(): MongoCollection = {
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("truenorth")
    val coll = db("users")
    coll
  }

  
  def getTrueNorthBookingDump2(): MongoCollection = {
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("truenorth")
    val coll = db("booking_dump2")
    coll
  }

  def getTrueNorthGeneralDashboard(): MongoCollection = {
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("truenorth")
    val coll = db("general_dashboard")
    coll
  }
  
  
  def getMongoCollection(client: MongoClient, collName: String): MongoCollection = {
    val db = client("truenorth")
    val coll = db(collName)
    coll
  }

}