import sbt.addArtifact

organization in ThisBuild := "ru.ertelecom.kafka.extract"
name := "deserializer"

version := "0.4"

resolvers in ThisBuild ++= Seq("Sonatype Nexus Repository Manager" at "https://spb99-nexus.ertelecom.ru/repository/maven-releases/")

scalaVersion in ThisBuild := "2.11.12"
pomIncludeRepository in ThisBuild := { _ => false }
credentials in ThisBuild += Credentials(Path.userHome / ".sbt" / ".credentials")
pomIncludeRepository in ThisBuild := { _ => false }
publishTo in ThisBuild := {
  val nexus = "https://spb99-nexus.ertelecom.ru/"
  if (isSnapshot.value) Some("snapshots" at nexus + "repository/maven-snapshots/")
  else Some("releases" at nexus + "repository/maven-releases/")
}
publishMavenStyle in ThisBuild := true
publishConfiguration in ThisBuild := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration in ThisBuild := publishLocalConfiguration.value.withOverwrite(true)
lazy val root = project.in(file("."))
  .settings(
    publishArtifact := false,
    publishArtifact in Test := false
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    pixel,
    pixel_pagetracking,
    pixel_wifi,
    recommendations,
    rmsi,
    routers,
    nat_gray,
    sniffers,
    netflow,
    tv_device,
    clickstream,
    pixel_info,
    dpi_camp_eql_logs,
    access_log_ad_equila,
    access_log_ad_equila_s,
    sms,
    web_domru,
    web_domru_al_calltouch,
    wifi_ruckus_syslog,
    wifi_portal_log,
    web_domru_billing_api,
    wifi_accnt,
    marker
  )

// MODULES
lazy val pixel = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-pixel",
    version := "0.5",
    libraryDependencies ++= coreDependencies
  )

lazy val pixel_pagetracking = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-pixelpagetracking",
    version := "1.2",
    libraryDependencies ++= pixelDependencies
  )

lazy val pixel_wifi = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-pixelwifi",
    version := "1.1",
    libraryDependencies ++= pixelDependencies
  )


lazy val recommendations = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-recommendations",
    version := "0.6",
    libraryDependencies := recommendationsDependencies
  )

lazy val routers = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-routers",
    version := "0.5",
    libraryDependencies ++= routersDependencies,
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.includePaths in Compile := Seq(file("routers/src/main/protobuf")),
    includeFilter in PB.generate := new SimpleFileFilter(
      (f: File) => f.getName.equals("message.proto")
    )
  )

lazy val nat_gray = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-nat-gray",
    version := "0.2",
    libraryDependencies ++= grayNatDependencies
  )

lazy val sniffers = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-sniffers",
    version := "2.2",
    libraryDependencies := sniffersDependencies
  )

lazy val rmsi = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-rmsi",
    version := "0.2",
    libraryDependencies := sniffersDependencies
  )

lazy val netflow = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-netflow",
    version := "0.5",
    libraryDependencies := netflowDependencies
  )

lazy val tv_device = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-tv_device",
    version := "1.1",
    libraryDependencies := coreDependencies
  )

lazy val clickstream = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-clickstream",
    version := "0.2",
    libraryDependencies := clickstreamDependencies
  )

lazy val pixel_info = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-pixel_info",
    version := "0.2",
    libraryDependencies ++= pixelInfoDependencies
  )

lazy val dpi_camp_eql_logs = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-dpi-camp-eql-logs",
    version := "0.9",
    libraryDependencies := eqlLogsDependencies
  )
lazy val access_log_ad_equila = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-access-log-ad-equila",
    version := "0.8",
    libraryDependencies := accessLogAdEquilaDependencies
  )

lazy val access_log_ad_equila_s = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-access-log-ad-equila-s",
    version := "0.5",
    libraryDependencies := accessLogAdEquilaDependenciesS
  ).dependsOn(access_log_ad_equila)

lazy val sms = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-sms",
    version := "0.7",
    libraryDependencies := smsDependencies
  )

lazy val web_domru = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-web-domru",
    version := "0.8",
    libraryDependencies := webDomRuDependencies
  )

lazy val web_domru_al_calltouch = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-web-domru-al-calltouch",
    version := "0.8",
    libraryDependencies := webDomRuAlCallTouchDependencies
  )

lazy val wifi_ruckus_syslog = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-wifi-ruckus-syslog",
    version := "0.6",
    libraryDependencies := recommendationsDependencies
  )

lazy val wifi_portal_log = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-wifi-portal-log",
    version := "0.6",
    libraryDependencies := wifiPortalLogDependencies
  )

lazy val web_domru_billing_api = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-web-domru-billing-api",
    version := "0.2",
    libraryDependencies := coreDependencies
  )

lazy val web_domru_sources = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-web-domru-sources",
    version := "0.4",
    libraryDependencies := pixelDependencies
  )

lazy val wifi_accnt = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-wifi_accnt",
    version := "0.7",
    libraryDependencies ++= wifiAccntDependencies
  )

lazy val pixel_pagetracking_b2b = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-pixelpagetracking-b2b",
    version := "0.1",
    libraryDependencies ++= pixelDependencies
  )

lazy val marker = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-marker",
    version := "0.2",
    libraryDependencies ++= markerDependencies
  )

lazy val gtm = project.settings(commonSettings: _*)
  .settings(
    name := "deserializer-gtm",
    version := "0.3",
    libraryDependencies ++= coreDependencies
  )

// DEPENDENCIES
lazy val dependencies = new {
  val coreVersion = "1.0"
  val sparkVersion = "2.3.2"
  val jacksonVersion = "2.7.9"

  val core = "ru.ertelecom.kafka.extract" %% "core" % coreVersion % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % Test

  val scalapb = "com.thesamet.scalapb" %% "compilerplugin" % "0.9.0" % Compile
  val playJson = "com.typesafe.play" % "play-json_2.11" % "2.4.6"
  val customUdfs = "ru.ertelecom" % "udf-bigdata" % "2.1"
  val uaParser = "ru.ertelecom.ua-parser" % "uap-java" % "1.4.4"
  val guava = "com.google.guava" % "guava" % "28.0-jre"
  val bouncyCastle = "org.bouncycastle" % "bcprov-jdk15on" % "1.64"
  val commonsCodec = "commons-codec" % "commons-codec" % "1.14"
}

lazy val coreDependencies = Seq(
  dependencies.sparkSql,
  dependencies.sparkCore,
  dependencies.core,
  dependencies.scalaTest
)

lazy val recommendationsDependencies = coreDependencies ++ Seq(
  dependencies.playJson
)

lazy val grayNatDependencies = coreDependencies ++ Seq(
  dependencies.customUdfs
)

lazy val sniffersDependencies = coreDependencies ++ Seq(
  dependencies.playJson

)

lazy val rmsiDependencies = coreDependencies ++ Seq(
  dependencies.playJson
)

lazy val netflowDependencies = coreDependencies ++ Seq(
  dependencies.customUdfs
)

lazy val routersDependencies = coreDependencies

lazy val clickstreamDependencies = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.guava,
  dependencies.customUdfs
)

lazy val pixelDependencies = coreDependencies ++ Seq(
  dependencies.uaParser
)

lazy val pixelInfoDependencies = coreDependencies

lazy val wifiAccntDependencies = coreDependencies ++ Seq(
  dependencies.playJson
)

lazy val eqlLogsDependencies = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.customUdfs
)
lazy val accessLogAdEquilaDependencies = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.customUdfs,
  dependencies.bouncyCastle,
  dependencies.commonsCodec
)

lazy val accessLogAdEquilaDependenciesS = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.customUdfs
)

lazy val smsDependencies = coreDependencies ++ Seq(
  dependencies.playJson
)

lazy val webDomRuAlCallTouchDependencies = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.customUdfs
)

lazy val webDomRuDependencies = coreDependencies ++ Seq(
  dependencies.uaParser,
  dependencies.playJson
)

lazy val wifiPortalLogDependencies = coreDependencies ++ Seq(
  dependencies.playJson,
  dependencies.customUdfs
)

lazy val markerDependencies = coreDependencies

// MODULES DEFAULT SETTINGS
lazy val commonSettings = Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  packagedArtifacts := Map.empty,
  artifact in(Compile, assembly) := {
    (artifact in(Compile, assembly)).value
  },
  addArtifact(artifact in(Compile, assembly), assembly),
  fork in Test := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
)