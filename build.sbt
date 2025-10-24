ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.example"

// Mantiene Spark 3.5.1
lazy val sparkVersion = "3.5.1"

val javaFixes = Seq(
  "--add-opens",
  "java.base/java.lang=ALL-UNNAMED",
  "--add-opens",
  "java.base/java.nio=ALL-UNNAMED",
  "--add-opens",
  "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens",
  "java.base/java.io=ALL-UNNAMED",
  "--add-opens",
  "java.base/sun.io=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "movies-etl",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "com.github.scopt" %% "scopt" % "4.1.0"
    ),
    javaOptions ++= javaFixes,
    Compile / run / fork := true,
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xms1g", "-Xmx1g") ++ javaFixes,
    Test / parallelExecution := false
  )

// sbt-assembly
assembly / test := {}
assembly / assemblyJarName := s"${name.value}_2.13-${version.value}.jar"
