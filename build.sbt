//project name
name := "assignment" 

//project version
version := "1.0"

//Spark 3.5.1 is published for Scala version 2.12
scalaVersion := "2.12.18"

// Spark library dependencies
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.5.1", 
	"org.apache.spark" %% "spark-sql" % "3.5.1"
)