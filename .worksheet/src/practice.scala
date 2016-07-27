import com.mongodb.casbah.Imports._

object practice {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(98); 
  println("Welcome to the Scala worksheet");$skip(71); 
  val x = MongoDBObject("$match" -> MongoDBObject("a" -> 1, "b" -> 2));System.out.println("""x  : com.mongodb.casbah.commons.Imports.DBObject = """ + $show(x ));$skip(61); 
  val y = MongoDBObject("$match" -> MongoDBObject("p" -> 5));System.out.println("""y  : com.mongodb.casbah.commons.Imports.DBObject = """ + $show(y ));$skip(17); 
  val z = x :: y;System.out.println("""z  : Seq[com.mongodb.casbah.commons.Imports.DBObject] = """ + $show(z ));$skip(17); 
  val s = x ++ y;System.out.println("""s  : com.mongodb.casbah.commons.Imports.DBObject = """ + $show(s ))}
  
}
