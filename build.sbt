name := "5parky"

organization := "org.hdfgroup"

// scalaHome := Some(file("/Users/hyoklee/scala/build/pack/"))
// scalaVersion := "2.12.4"
scalaVersion := "2.11.8"
ensimeScalaVersion in ThisBuild := "2.11.8"

scalacOptions += "-deprecation"

crossScalaVersions := Seq("2.10.6", "2.11.8")
// crossScalaVersions := Seq("2.12.4", "2.11.8")

spName := "hdfgroup/5parky"

// sparkVersion := "2.0.0"
sparkVersion := "2.3.2"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// Display full-length stacktraces from ScalaTest:
// testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.8")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value

parallelExecution in Test := false
