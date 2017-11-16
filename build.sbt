name := "spark-lite"

version := "0.0.1"

scalaVersion := "2.11.11"

organization := "com.github.mikaelv"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies ++= Seq("org.specs2" %% "specs2-core" % "3.9.5" % "test")

scalacOptions in Test ++= Seq("-Yrangepos")

// https://github.com/sbt/sbt/issues/3570
updateOptions := updateOptions.value.withGigahorse(false)

useGpg := false
usePgpKeyHex("AB325B7B29AC50FF")
pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.gpg"
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.gpg"
pgpPassphrase := sys.env.get("PGP_PASS").map(_.toArray)

sonatypeProfileName := organization.value

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("SONATYPE_USER", ""),
  sys.env.getOrElse("SONATYPE_PASS", "")
)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
homepage := Some(url("https://github.com/mikaelv/spark-lite"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/mikaelv/spark-lite"),
    "scm:git@github.com:mikaelv/spark-lite.git"
  ))

developers := List(
  Developer(
    id="mikaelv",
    name="Mikael Valot",
    email="info@valdev.co.uk",
    url=url("http://valdev.co.uk")
  ))

enablePlugins(GitVersioning)

/* The BaseVersion setting represents the in-development (upcoming) version,
 * as an alternative to SNAPSHOTS.
 */
git.baseVersion := version.value

val ReleaseTag = """^v([\d\.]+)$""".r
git.gitTagToVersionNumber := {
  case ReleaseTag(v) => Some(v)
  case _ => None
}

git.formattedShaVersion := {
  val suffix = git.makeUncommittedSignifierSuffix(git.gitUncommittedChanges.value, git.uncommittedSignifier.value)

  git.gitHeadCommit.value map { _.substring(0, 7) } map { sha =>
    git.baseVersion.value + "-" + sha + suffix
  }
}

addCommandAlias("ci-all",  ";+clean ;+compile ;+test ;+package")
addCommandAlias("release", ";+publishSigned ;sonatypeReleaseAll")


