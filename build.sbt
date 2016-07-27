lazy val root = (project in file(".")).
  settings(
    name := "tdp", 
    version := "1.0.01", 
    scalaVersion := "2.11.7",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.+",
    libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.+",
    libraryDependencies += "org.mongodb" %% "casbah" % "3.1.0"
  )
