name := "clin-variant-etl"

version := "0.1"

scalaVersion := "2.12.13"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.0.2"

resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases" //faster than waiting for https://repo1.maven.org/maven2

/* Runtime */
libraryDependencies += "bio.ferlab" %% "datalake-core" % "0.0.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1" % Provided
libraryDependencies += "io.projectglow" %% "glow-spark3" % "0.6.0" exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "io.delta" %% "delta-core" % "0.8.0"

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"

parallelExecution in test := false
fork := true

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
assemblyJarName in assembly := "clin-variant-etl.jar"
