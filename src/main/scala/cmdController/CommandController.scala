package cmdController

import dataModels.SQLFinanceDataWriter
import dataModels.truenorth._
import helpers.{Calculator}
import java.util.concurrent._ 
import com.mongodb.casbah.Imports._
import connUtil.nosql.{MongoConnection}

object CommandController {
  
  //Authorization Methods Executor
  def cmdAuthExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "tn:usr-act" => {
        println("TrueNorth User Activation by username")
        true
      }
      case "tn:usr-act-all" => {
        println("Starting Authorization...")
        val pool: ExecutorService = Executors.newCachedThreadPool()
        val users: Iterator[DBObject] = MongoQueries.fetchAllApprovedUsers
        for (user <- users /*if user("username") == "jeydurai"*/) {
          pool.execute(new Authorization(user, MongoConnection().getMongoClient))
        }
        pool.shutdown()
        try {
          val taskEnded: Boolean = pool.awaitTermination(1, TimeUnit.HOURS)
          val whatHappened: String = taskEnded match {
            case true => "Tasks Ended"
            case false => "Timed out while waiting for tasks to finish"
          }
          println(); println(whatHappened)
        } catch {
          case e: InterruptedException  => {
            println()
            println("Interrupted while waiting for tasks to finish")
          }
        } finally {
            println("Finally clause executed")
        }
        true
      }
      case "tn:usr-dac" => {
        println("TrueNorth User Deactivation by username")
        true
      }
      case "tn:usr-ref" => MongoUpdater.removeMappedToField(MongoConnection().getTrueNorthBookingDump2())
      case "tn:usr-res" => MongoUpdater.setUsersApproved(MongoConnection().getTrueNorthUsers())
      case _ => {
        println("Options Missing in 'auth' command!")
        true
      }
    }
    isSuccessful
  }

    // Conversion Methods Executor
    def cmdCvrtExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "fd:xlsq-sq:sq-nsq" => SQLFinanceDataWriter.convertAndWrite
      case "bd:sq-nsq" => SQLFinanceDataWriter.convertAsSnapshotAndWrite
      case "fd:xlsq-init" => SQLFinanceDataWriter.BookingDataInit
      case _ => {
        println("Options Missing in 'cvrt' command!")
        true
      }
    }
    isSuccessful
  }

    // Backup Methods Executor
    def cmdBackupExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "all:nsq-nsq-w" => {
        println("Take Backup all in NoSQL from W-Series Laptop")
        true
      }
      case "all:nsq-nsq-x" => {
        println("Take Backup all in NoSQL from X1 Carbon Laptop")
        true
      }
      case _ => {
        println("Options Missing in 'bkup' command!")
        true
      }
    }
    isSuccessful
  }

    // Restore Methods Executor
    def cmdRestoreExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "coll:nsq-nsq-w" => {
        println("Restore specific collections in NoSQL from W-Series Laptop")
        true
      }
      case "coll:nsq-nsq-x" => {
        println("Restore specific collections in NoSQL from X1 Carbon Laptop")
        true
      }
      case "all:nsq-nsq-w" => {
        println("Restore all collections in NoSQL from W-Series Laptop")
        true
      }
      case "all:nsq-nsq-x" => {
        println("Restore all collections in NoSQL from X1 Carbon Laptop")
        true
      }
      case _ => {
        println("Options Missing in 'rstr' command!")
        true
      }
    }
    isSuccessful
  }

    // Restore Methods Executor
    def cmdSubsetExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "bd:nsq-nsq-und" => {
        println("Sub set NoSQL Booking Data to write Uniquenodes in NoSQL")
        true
      }
      case "bd:nsq-nsq-gd" => {
        val pool: ExecutorService = Executors.newCachedThreadPool()
        val users: Iterator[DBObject] = MongoQueries.getAllApprovedUsers
        for (user <- users /*if user("username") == "jeydurai"*/) {
          pool.execute(new GeneralDashboard(user, MongoConnection().getMongoClient))
        }
        pool.shutdown()
        try {
          val taskEnded: Boolean = pool.awaitTermination(1, TimeUnit.HOURS)
          val whatHappened: String = taskEnded match {
            case true => "Tasks Ended"
            case false => "Timed out while waiting for tasks to finish"
          }
          println(); println(whatHappened)
        } catch {
          case e: InterruptedException  => {
            println()
            println("Interrupted while waiting for tasks to finish")
          }
        } finally {
            println("Finally clause executed")
        }
        true
      }
      case "gd:nsq-nsq-gdyoy" => {
        println("Subset NoSQL General Dashboard Data to write YoY in general_dashboard2 data")
        true
      }
      case "bd:nsq-nsq-gdps" => {
        val pool: ExecutorService = Executors.newCachedThreadPool()
        val users: Iterator[DBObject] = MongoQueries.getAllApprovedUsers
        for (user <- users /*if user("username") == "jeydurai"*/) {
          pool.execute(new GeneralDashboardProdSer(user, MongoConnection().getMongoClient))
        }
        pool.shutdown()
        try {
          val taskEnded: Boolean = pool.awaitTermination(1, TimeUnit.HOURS)
          val whatHappened: String = taskEnded match {
            case true => "Tasks Ended"
            case false => "Timed out while waiting for tasks to finish"
          }
          println(); println(whatHappened)
        } catch {
          case e: InterruptedException  => {
            println()
            println("Interrupted while waiting for tasks to finish")
          }
        } finally {
            println("Finally clause executed")
        }
        true
      }
      case "bd:nsq-nsq-gdss" => {
        val pool: ExecutorService = Executors.newCachedThreadPool()
        val users: Iterator[DBObject] = MongoQueries.getAllApprovedUsers
        for (user <- users /*if user("username") == "jeydurai"*/) {
          pool.execute(new GeneralDashboardSubSCMS(user, MongoConnection().getMongoClient))
        }
        pool.shutdown()
        try {
          val taskEnded: Boolean = pool.awaitTermination(1, TimeUnit.HOURS)
          val whatHappened: String = taskEnded match {
            case true => "Tasks Ended"
            case false => "Timed out while waiting for tasks to finish"
          }
          println(); println(whatHappened)
        } catch {
          case e: InterruptedException  => {
            println()
            println("Interrupted while waiting for tasks to finish")
          }
        } finally {
            println("Finally clause executed")
        }
        true
      }
      case "gd:nsq-nsq-gdprodyoy" => {
        println("Subset NoSQL General Dashboard Data to write YoY in general_dashboard2 data for Product")
        true
      }
      case "gd:nsq-nsq-gdservyoy" => {
        println("Subset NoSQL General Dashboard Data to write YoY in general_dashboard2 data for Service")
        true
      }
      case _ => {
        println("Options Missing in 'subs' command!")
        true
      }
    }
    isSuccessful
  }

    // Help Methods Executor
    def cmdHelpExecutor(optionString: String): Boolean = {
    val isSuccessful: Boolean = optionString match {
      case "cvrt" => {
        println( "\nConverter Command")
        println( "=====================")
        println( "What is it for? : To convert any form of data to a specific form")
        println( "Options Available:")
        println( "===================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'fd:xlsq-sq:sq-nsq': used to convert finance dump from XL to SQL and SQL to NoSQL (MongoDB)")
        println( "'bd:sq-nsq': used to convert booking dump from SQL to NoSQL in a snapshot (MongoDB)")
        println( "'fd:xlsq-init': used to initialize the datasource for data conversion")
        println( "==============================================================================================")
        true
      }
      case "auth" => {
        println( "\nConverter Command")
        println( "====================")
        println( "What is it for? : To Authorize TrueNorth data")
        println( "Options Available:")
        println( "===================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'tn:usr-act': to authorize/activate/map a user to the booking_dump table")
        println( "'tn:usr-act-all': to authorize/activate/map all users to the booking_dump table")
        println( "'tn-usr-dac': to unauthorize/deactivate/unmap a users to the booking_dump table")
        println( "'tn:usr-ref': to refresh all users by unmap the MapTo field in the booking_dump table")
        println( "'tn:usr-res': to rollback all users' activation by unset the approval status code to 2 in Users collection")
        println( "=============================================================================================")
        true
      }
      case "modi" => {
        println( "\nModification/Edit Command")
        println( "===========================")
        println( "What is it for? : To create Indexes and do other modifications in booking & dashboard data")
        println( "Options Available:")
        println( "===================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'bd:nsq-idx': to create indexes in booking dump collection in MongoDB")
        println( "'bd:nsq-idx-rm': to remove indexes from booking dump collection in MongoDB")
        println( "'gd:nsq-idx': to create indexes in general dashboard data collection in MongodDB")
        println( "===========================================================================================")
        true
      }
      case "subs" => {
        println( "\nSubset Data Command")
        println( "=====================")
        println( "What is it for? : To subset data from booking dump, goal sheet, funnel report")
        println( "Options Available:")
        println( "==================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'bd:nsq-nsq-und': to subset unique nodes from booking dump and write it in separate collection")
        println( "'bd:nsq-nsq-gd': to subset general dashboard data from booking dump and write it in separate collection")
        println( "'gd:nsq-nsq-gdyoy': to subset YoY data from general_dashboard data and write it in separate collection")
        println( "'bd:nsq-nsq-gdps': to subset Product/Service general dashboard data from booking dump and write it in separate collection")
        println( "'bd:nsq-nsq-gdss': to subset Sub_SCMS general dashboard data from booking dump and write it in separate collection")
        println( "'gd:nsq-nsq-gdprodyoy': to subset YoY data from Product general_dashboard data and write it in separate collection")
        println( "'gd:nsq-nsq-gdservyoy': to subset YoY data from Service general_dashboard data and write it in separate collection")
        println( "===========================================================================================================================")
        true
      }
      case "bkup" => {
        println( "\nData Backup Command")
        println( "=====================")
        println( "What is it for? : To take backup in MongoDB for 'truenorth' database")
        println( "Options Available:")
        println( "==================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'all:nsq-nsq-w': to take backup of all collections in truenorth database from W-Series Laptop")
        println( "'all:nsq-nsq-x': to take backup of all collections in truenorth database from X1-Carbon Laptop")
        println( "=================================================================================================")
        true
      }
      case "rstr" => {
        println( "\nData Restore Command")
        println( "======================")
        println( "What is it for? : To Restore Dump in MongoDB for 'truenorth' database")
        println( "Options Available:")
        println( "===================")
        println( "<table>:<fileFormat>-<fileFormat>-[action]")
        println( "'coll:nsq-nsq-w': to restore set of collections in truenorth database from W-Series Laptop")
        println( "'coll:nsq-nsq-x': to restore set of collections in truenorth database from X1-Carbon Laptop")
        println( "'all:nsq-nsq-w': to restore all collections in truenorth database from W-Series Laptop")
        println( "'all:nsq-nsq-x': to restore all collections in truenorth database from X1-Carbon Laptop")
        println( "============================================================================================")
        true
      }
      case _ => {
        println( "\nCommands available")
        println( "=====================")
        println( "<command> <table>:<fileFormat>-<fileFormat>-[action]")
        println( "'auth': Authorization command")
        println( "'cvrt': Conversion command")
        println( "'modi': Modification/Edit command")
        println( "'subs': Subset Data command")
        println( "'bkup': Data Backup command")
        println( "'rstr': Data Restore command")
        println( "'help': To list all available commands in JDP")
        println( "=============================================================")
        true
      }
    }
    isSuccessful
  }

    
}