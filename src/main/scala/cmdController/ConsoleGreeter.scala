package cmdController

object ConsoleGreeter {
  private val HEADER: String = " WELCOME TO TrueNorth DATA PROCESSOR "
  private val SUB_HEADER1: String = " Version 1.01.01"
	private val SUB_HEADER2: String = " a unique Data Processor Console Application";
	private val SUB_HEADER3: String = " Owner: D. Jeyaraj";
	private val SUB_HEADER4: String = " Divsion: Commercial";
	private val SUB_HEADER5: String = " Profile: Data Analytics";
	private val SUB_HEADER6: String = " TDP is meant to be for internal use only and doesn't have any copyright";

	private val FOOTER: String = " THANK YOU FOR USING TrueNorth DATA PROCESSOR ";
	private val SUB_FOOTER1: String = " Hope you have had a fun using this tool";
	private val SUB_FOOTER2: String = " TDP is still under modern development";
	private val SUB_FOOTER3: String = " New versions will have more features that will ease you in accomplishing your data conversion";
	private val SUB_FOOTER4: String = " 'Imagination is more important than knowledge -- Albert Einstein'";
	private val SUB_FOOTER5: String = " You can write to us your feedbacks to jeydurai@cisco.com";
	
	private val TOTAL_LENGTH: Int = 90;
	private val SHORT_LENGTH: Int = 10;
	private val PAGE_SIZE: Int = 30; 
  

	def putsb: Unit = println()
	def putsl(str: String): Unit = println(str)
	def puts(str: String): Unit = print(str)
	def lenSideDecorator(text: String, length: Int): Int = (this.TOTAL_LENGTH - text.length())/length
	def clearScreen(): Unit = System.out.flush()
	def clearScreen2(): Unit = for(i <- 0 to this.PAGE_SIZE) println()
	def rptString(str: String, times: Int): String = {
	  val strBuilder: StringBuilder = new StringBuilder()
	  for (i <- 0 to times) strBuilder.append(str)
	  strBuilder.toString()
	}
	
	def prompt: Unit = this.puts("<tdp::jeydurai@cisco.com>> ")
	def printPrompt(shouldIncludeHeaderFooter: Boolean) = {
	  if (shouldIncludeHeaderFooter) {
  	  clearScreen2()
  	  printWelcomeMessage()
  	  prompt
	  } else {
	    putsb
	    prompt
	  }
	  
	}
	def printWelcomeMessage(): Unit = {
	  var tempLength: Int = this.SHORT_LENGTH
	  //Header
	  putsb
	  putsl(rptString("=", this.TOTAL_LENGTH))
	  putsb
	  puts(rptString("*", lenSideDecorator(this.HEADER, 2)))
	  puts(this.HEADER)
	  puts(rptString("*", lenSideDecorator(this.HEADER, 2)))
	  putsb
	  putsl(rptString("=", this.TOTAL_LENGTH))
	  //Side Header1
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER1)
	  //Side Header2
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER2)
	  //Side Header3
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER3)
	  //Side Header4
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER4)
	  //Side Header5
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER5)
	  //Side Header6
	  tempLength -= 1; puts(rptString("#", tempLength))
	  putsl(this.SUB_HEADER6)
	  putsl(rptString("=", this.TOTAL_LENGTH))
	}
	
	def printExitMessage(): Unit = {
	  var tempLength: Int = SHORT_LENGTH;
		//Footer
		putsl(rptString("=", this.TOTAL_LENGTH));
		putsb
		tempLength -= 1; puts(rptString("*",  tempLength))
		puts(this.FOOTER);
		tempLength -= 1; putsl(rptString("*",  tempLength))
		putsb
		putsl(rptString("=", this.TOTAL_LENGTH))
		//Sub Footer1
		tempLength -= 1; puts(rptString("#",  tempLength))
		putsl(this.SUB_FOOTER1)
		//Sub Footer2
		tempLength -= 1; puts(rptString("#",  tempLength))
		putsl(this.SUB_FOOTER2)
		//Sub Footer3
		tempLength -= 1; puts(rptString("#",  tempLength))
		putsl(this.SUB_FOOTER3);
		//Sub Footer4
		tempLength -= 1; puts(rptString("#",  tempLength))
		putsl(this.SUB_FOOTER4)
		//Sub Footer5
		tempLength -= 1; puts(rptString("#",  tempLength))
		putsl(this.SUB_FOOTER5)
		putsl(rptString("=", this.TOTAL_LENGTH))

	}
	
}