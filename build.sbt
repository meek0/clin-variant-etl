name := "clin-variant-etl"

version := "0.1"

scalaVersion := "2.12.13"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.1.2"
val deltaCoreVersion = "1.0.0"
val scalatestVersion = "3.2.9"
val glowVersion = "1.0.1"
val datalakeSpark3Version = "0.2.37"
val elasticsearchVersion = "7.15.0"

resolvers += "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots"
resolvers += "Sonatype OSS Releases" at "https://s01.oss.sonatype.org/content/repositories/releases" //faster than waiting for https://repo1.maven.org/maven2

/* Runtime */
libraryDependencies += "bio.ferlab" %% "datalake-spark31" % datalakeSpark3Version
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.0" % Provided
libraryDependencies += "io.projectglow" %% "glow-spark3" % glowVersion exclude ("org.apache.hadoop", "hadoop-client")
libraryDependencies += "io.delta" %% "delta-core" % deltaCoreVersion
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-30" % elasticsearchVersion
dependencyOverrides ++= Seq(
  //"org.antlr" % "antlr4" % "4.9.2",
  "org.antlr" % "antlr4-runtime" % "4.8",
  "org.antlr" % "antlr4-tool" % "4.7.1"
)

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % Test

test / parallelExecution := false
fork := true

assembly / test:= {}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
  case "META-INF/DISCLAIMER" => MergeStrategy.last
  case "mozilla/public-suffix-list.txt" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.first
  case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
assembly / assemblyJarName := "clin-variant-etl.jar"
