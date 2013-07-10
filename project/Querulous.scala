
import sbt._
import Keys._

object Querulous extends Build {

	lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    	organization := "com.ticketfly",
    	version      := "0.1.0",
    	scalaVersion := "2.10.2",
    	crossPaths   := true )

	val mysql = "mysql"        % "mysql-connector-java" % "5.1.24"
  val dbcp  = "commons-dbcp" % "commons-dbcp"         % "1.4"
  val pool  = "commons-pool" % "commons-pool"         % "1.5.4"


	lazy val core = Project("querulous-core", 
		base     = file("querulous-core"),
		settings = buildSettings ++ Seq(
      libraryDependencies ++= mysql :: dbcp :: pool :: Nil
      ))


}