scalaVersion := "2.11.12"
name := "data-profiler-for-aws-glue-data-catalog"
organization := "aws"
version := "1.0"

libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.2"

// aws-java-sdk version 1.11.659 is the one provided with EMR 5.28.0
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.659" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.8.5"

// https://mvnrepository.com/artifact/com.github.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}
