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

test in assembly := {}

lazy val myPackage = "ai.tripl.arc"
lazy val relocationPrefix = s"$myPackage.repackaged"

val excludedOrgs = Seq(
  // All use commons-cli:1.4
  "commons-cli",
  // Not a runtime dependency
  "com.google.auto.value",
  // All use jsr305:3.0.0
  "com.google.code.findbugs",
  "javax.annotation",
  // Spark Uses 2.9.9 google-cloud-core uses 2.9.2
  "com.sun.jdmk",
  "com.sun.jmx",
  "javax.activation",
  "javax.jms",
  "javax.mail"
)

lazy val renamed = Seq(
  "android",
  "avro.shaded",
  "com.fasterxml",
  "com.google.common",
  "com.google.api",
  "com.google.auth",
  "com.google.auto",
  "com.google.cloud.audit",
  "com.google.code",
  "com.google.errorprone",
  "com.google.http-client",
  "com.google.geo",
  "com.google.gson",
  "com.google.iam",
  "com.google.logging",
  "com.google.longrunning",
  "com.google.thirdparty",
  "com.google.rpc",
  "com.google.type",
  "com.google.j2objc",
  "com.google.protobuf",
  "org.apache.commons",
  "org.apache.http",
  "io.grpc",
  "io.opencensus",
  "javax.annotation",
  "org.threeten",
  "org.conscrypt",
  "org.checkerframework",
  "io.perfmark",
  "io.codehaus.mojo",
  "org.aopalliance",
  "org.codehaus",
  "com.google.cloud.datacatalog"
)
lazy val notRenamed = Seq(myPackage, "com.google.cloud.spark.bigquery")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := (
      notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
        ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
      ).map(_.inAll)

// META-INF discarding
assemblyMergeStrategy in assembly := {
   {
      case x if x.endsWith("/public-suffix-list.txt") => MergeStrategy.filterDistinctLines
      case "module-info.class" => MergeStrategy.discard
      case PathList(ps@_*) if ps.last.endsWith(".properties") => MergeStrategy.filterDistinctLines
      case PathList(ps@_*) if ps.last.endsWith(".proto") => MergeStrategy.discard
      // Relocate netty-tcnative.so. This is necessary even though gRPC shades it, because we shade
      // gRPC.
      /*case PathList("META-INF", "native", f) if f.contains("netty_tcnative") =>
        RelocationMergeStrategy(path =>
          path.replace("native/lib", s"native/lib${relocationPrefix.replace('.', '_')}_"))*/
      case PathList("META-INF", "native", f) => MergeStrategy.first

      // Relocate GRPC service registries
      case PathList("META-INF", "services", _) => ServiceResourceMergeStrategy(renamed,
        relocationPrefix)
      case x => (assemblyMergeStrategy in assembly).value(x)
   }
}
