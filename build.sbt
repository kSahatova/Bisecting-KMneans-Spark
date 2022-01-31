import sbt.Def.settings
import sbtassembly.AssemblyPlugin.assemblySettings

name := "bd-project"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.1"

assemblyJarName in assembly := "project_kmeans.jar"

mainClass in Compile := Some("BKMeans_rdd")


lazy val commonSettings = Seq(
  version := "0.1",
  organization := "sahatova",
  scalaVersion := "2.12.15",
  test in assembly := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("BKMeans_rdd")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "project_kmeans_medium.jar"

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value
