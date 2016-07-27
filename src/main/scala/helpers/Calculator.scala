package helpers

object Calculator {

  def THOUSAND = 1000
  def MILLION = 1000000

  def calculateDiscount[A, T](args: Tuple2[A, T]): Double = {
    val returnValue: Double = args match {
      case (n: Int, d: Int) => (1-(n.toDouble/d.toDouble))
      case (n: Float, d: Float) => (1-(n.toDouble/d.toDouble))
      case (n: Double, d: Double) => (1-(n/d))
      case (n: Double, d: Int) => (1-(n/d.toDouble))
      case (_, _) => 0d
    }
    returnValue
  }
  
  def calculateGrowth[A, T](args: Tuple2[A, T]): Double = {
    val returnValue: Double = args match {
      case (n: Int, d: Int) if ((n<0 && d>0) || (n>0 && d<0))  => 0d
      case (n: Float, d: Float) if ((n<0f && d>0f) || (n>0f && d<0f))  => 0d
      case (n: Double, d: Double) if ((n<0d && d>0d) || (n>0d && d<0d))  => 0d
      case (n: Double, d: Int) if ((n<0d && d>0) || (n>0d && d<0))  => 0d
      case (n: Int, d: Int)  => n.toDouble/d.toDouble
      case (n: Float, d: Float) => n.toDouble/d.toDouble
      case (n: Double, d: Double) => n/d
      case (n: Double, d: Int) => n/d
      case (_, _) => 0d
    }
    returnValue
  }

  def calculateRatio[A, T](args: Tuple2[A, T]): Double = {
    val returnValue: Double = args match {
      case (n: Int, d: Int) => n.toDouble / d.toDouble
      case (n: Float, d: Float) => n.toDouble/d.toDouble
      case (n: Double, d: Double) => n/d
      case (n: Double, d: Int) => n/d.toDouble
      case (_, _) => 0d
    }
    returnValue
  }

  
}