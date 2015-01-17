name := "ADFS"

version := "0.0.1"

scalaVersion := "2.11.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:postfixOps")

val akkaVersion = "2.3.6"

libraryDependencies ++=
  Seq(
    "com.typesafe.akka"          %% "akka-actor"                    % akkaVersion,
    "com.typesafe.akka"          %% "akka-cluster"                  % akkaVersion,
    "com.typesafe.akka"          %% "akka-testkit"                  % akkaVersion,
    "org.iq80.leveldb"            % "leveldb"                       % "0.7",
    "org.scala-lang"             %% "scala-pickling"                % "0.9.0",
    "org.fusesource.leveldbjni"   % "leveldbjni-osx"                % "1.8",                             // New BSD
    "org.scalatest"              %% "scalatest"                     % "2.2.1"               % "test",    // ApacheV2,
    "com.typesafe.play"          %% "play-json"                     % "2.3.4"

  )

fork := true
