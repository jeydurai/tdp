import com.mongodb.casbah.Imports._

object practice {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  val x = MongoDBObject("$match" -> MongoDBObject("a" -> 1, "b" -> 2))
                                                  //> SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
                                                  //| SLF4J: Defaulting to no-operation (NOP) logger implementation
                                                  //| SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further de
                                                  //| tails.
                                                  //| x  : com.mongodb.casbah.commons.Imports.DBObject = { "$match" : { "a" : 1 , 
                                                  //| "b" : 2}}
  val y = MongoDBObject("$match" -> MongoDBObject("p" -> 5))
                                                  //> y  : com.mongodb.casbah.commons.Imports.DBObject = { "$match" : { "p" : 5}}
                                                  //| 
  val z = x :: y                                  //> z  : Seq[com.mongodb.casbah.commons.Imports.DBObject] = List({ "$match" : { 
                                                  //| "a" : 1 , "b" : 2}}, { "$match" : { "p" : 5}})
  val s = x ++ y                                  //> s  : com.mongodb.casbah.commons.Imports.DBObject = { "$match" : { "p" : 5}}
                                                  //| 
  
}