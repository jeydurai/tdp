package dataModels

import java.sql.{Connection, PreparedStatement, ResultSet}

trait BookingDataFieldAdder {

  
  def getUniqueCustomerVertical(conn: Connection, param: String): Tuple2[String, String] = {
    var customerName: String = ""; var vertical: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT DISTINCT unique_names, vertical FROM universal_unique_names WHERE names=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_unique_names' ...")
    pst.setString(1, param)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      customerName = resultSet.getString("unique_names")
      vertical = resultSet.getString("vertical")
    }
    val returnString: Tuple2[String, String] = (customerName, vertical)
    returnString    
  }
  
  def getUniquePartner(conn: Connection, param: String): String = {
    var returnString: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT DISTINCT unique_names, vertical FROM universal_unique_names WHERE names=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_unique_names' ...")
    pst.setString(1, param)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      returnString = resultSet.getString("unique_names")
    }
    returnString    
  }

  def getCorrectSubSCMS(conn: Connection, param: String, param2: Int, param3: String): String = {
    var returnString: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT DISTINCT sub_scms FROM unique_sub_scms WHERE customer_name=? AND fp_year=? AND sales_level_6=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_unique_names' ...")
    pst.setString(1, param); pst.setInt(2, param2); pst.setString(3, param3);
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      returnString = resultSet.getString("sub_scms")
    }
    returnString    
  }
   
   
   def getUniqueState(conn: Connection, param: String): String = {
    var returnString: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT DISTINCT sales_level_6, unique_states FROM unique_states WHERE sales_level_6=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_states' ...")
    pst.setString(1, param)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      returnString = resultSet.getString("unique_states")
    }
    returnString    
  }

   def getUniqueRegionAndGTMu(conn: Connection, param: String): Tuple2[String, String] = {
    var gtmu: String = ""; var region: String = ""  
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT region, gtmu FROM node_mapper WHERE sales_level_5 = ?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.node_mapper' ...")
    pst.setString(1, param)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      gtmu = resultSet.getString("gtmu")
      region = resultSet.getString("region")
    }
    val returnString: Tuple2[String, String] = (gtmu, region)
    returnString
  }

  def cleanSubSCMS(str: String): String = {
    val pattern = """.*(PL_S|PL|COM-OTHER|COM-MM|GEO_NAMED|GEO_NON_NA|SELECT).*""".r
    val result: String = str match { case pattern(m) => m; case _ => "COMM"}
    result
  }

  def getTechnologies(conn: Connection, param: String): Tuple3[String, String, String] = {
    var tech_name: String = ""; var arch1: String = "";  var arch2: String = ""  
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT DISTINCT tech_name_1, arch1, arch2 FROM tech_grand_master WHERE tech_code=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.tech_grand_master' ...")
    pst.setString(1, param)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      tech_name = resultSet.getString("tech_name_1")
      arch1 = resultSet.getString("arch1")
      arch2 = resultSet.getString("arch2")
    }
    val returnString: Tuple3[String, String, String] = (tech_name, arch1, arch2)
    returnString
  }

  def getIOTPortfolio(conn: Connection, param: String, param2: String): String = {
    var returnString: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT iot_portfolio FROM iot_portfolios WHERE Product_Fam_id = ? OR Product_Fam_id = ?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_states' ...")
    pst.setString(1, param); pst.setString(2, param2)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      returnString = resultSet.getString("iot_portfolio")
    }
    returnString    
  }
  
  def getWeek(conn: Connection, param: String, param2: String): String = {
    var returnString: String = ""
    val pst: PreparedStatement = conn.prepareStatement("""
      |SELECT fp_week FROM week_master WHERE fp_quarter=? AND week_in_database=?
      """.stripMargin.replaceAll("\n", " "))
    //println("Querying 'mysourcedata.universal_states' ...")
    pst.setString(1, param); pst.setString(2, param2)
    val resultSet: ResultSet = pst.executeQuery()
    while (resultSet.next()) {
      returnString = resultSet.getString("fp_week")
    }
    returnString    
  }

  def extractTechCode(conn: Connection, finYear: Int, tmsL1: String, subBEName: String): String = {
    var techCode: String = subBEName
    if (finYear <= 2012) techCode = tmsL1.slice(tmsL1.indexOf("-")+1, tmsL1.length)
    techCode
  }
  
 
}