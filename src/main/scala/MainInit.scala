import scala.util.control._
import cmdController.ConsoleGreeter._
import cmdController.CommandController

object MainInit {
  
  def main(args: Array[String]) { 
    val loop = new Breaks;
    var continueLoop: Boolean = true;
    printPrompt(true)
    loop.breakable {
      while (continueLoop) {
        //print("jdp::jeydurai@cisco.com: ")
        val line = io.StdIn.readLine()
        val cmdPattern = """(\w+)\s+[-](.*)""".r
        val matchList = cmdPattern.findAllIn(line).matchData.toList
        var cmd = ""; var optionString = "";
        if (!matchList.isEmpty) { // Test whether the command has got syntax correctly
          val firstMatch = matchList(0)
          cmd = firstMatch.group(1)
          optionString = firstMatch.group(2)
        } else {
          val cmdPattern2 = """(.*)""".r
          val matchList2 = cmdPattern2.findAllIn(line).matchData.toList
          if (!matchList2.isEmpty) { // Test whether the command is without the option
            val firstMatch2 = matchList2(0)
            cmd = firstMatch2.group(1)
          }
        }
        continueLoop = cmd match {
          case "auth" => CommandController.cmdAuthExecutor(optionString)
          case "cvrt" => CommandController.cmdCvrtExecutor(optionString)
          case "modi" => true 
          case "subs" => CommandController.cmdSubsetExecutor(optionString)
          case "bkup" => CommandController.cmdBackupExecutor(optionString)
          case "rstr" => CommandController.cmdRestoreExecutor(optionString)
          case "help" => CommandController.cmdHelpExecutor(optionString)
          case "quit" => {
            println("you are now quitting...")
            printExitMessage
            false
          }
          case _ => {
            println("Your input is: " + line)
            true
          }
        }
        printPrompt(false)
      }
    }
  }
}
