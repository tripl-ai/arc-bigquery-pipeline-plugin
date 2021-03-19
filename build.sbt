import Dependencies._

lazy val scala212 = "2.12.12"
lazy val supportedScalaVersions = List(scala212)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc-big-query-pipeline-plugin",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    crossScalaVersions := supportedScalaVersions,
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.bigquery",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false),
    resolvers += "Sonatype OSS Staging" at "https://oss.sonatype.org/content/groups/staging"
  )

fork in run := true

resolvers += Resolver.mavenLocal
publishM2Configuration := publishM2Configuration.value.withOverwrite(true)

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")
