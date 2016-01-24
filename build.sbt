name := "Advanced Analytics with Spark"

organization := "com.hablapps"

version := "1.0"

scalaVersion := "2.10.4"

EclipseKeys.withSource := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.6.0"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.10"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.11.2"



