name := "linear-regression-spark-ml"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "3.0.1" withSources(),
  "org.apache.spark" %% "spark-sql" % "3.0.1" withSources(),
  "org.scalatest" %% "scalatest" % "3.2.2" % "test" withSources()
)
