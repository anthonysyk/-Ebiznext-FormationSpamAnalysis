name := "FormationDataScience"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.1"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.nscala-time" %% "nscala-time" % "2.12.0",
  "com.quantifind" %% "wisp" % "0.0.4",
  "joda-time" % "joda-time" % "2.9.4",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.jfree" % "jfreechart" % "1.0.19"
)