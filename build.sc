import mill._, scalalib._

object foo extends RootModule with ScalaModule {
  def scalaVersion = "3.3.1"
  def ivyDeps = Agg(
    ivy"com.lihaoyi::scalatags:0.12.0",
    ivy"com.typesafe:config:1.4.3",
    ivy"co.fs2::fs2-core:3.9.3",
    ivy"co.fs2::fs2-io:3.9.3",
    ivy"org.apache.commons:commons-compress:1.25.0",
    ivy"commons-codec:commons-codec:1.16.0",
    ivy"com.typesafe.scala-logging::scala-logging:3.9.4",
    ivy"ch.qos.logback:logback-classic:1.2.3",
    ivy"org.typelevel::log4cats-slf4j:2.6.0".withDottyCompat(scalaVersion()),
    ivy"org.mongodb.scala::mongo-scala-driver:4.4.0".withDottyCompat(scalaVersion()),
    ivy"io.circe::circe-core:0.14.1",
    ivy"io.circe::circe-generic:0.14.1",
    ivy"io.circe::circe-parser:0.14.1",
    ivy"com.github.scopt::scopt:4.1.0"
  )

  object test extends ScalaTests {
    def ivyDeps = Agg(ivy"com.lihaoyi::utest:0.7.11")
    def testFramework = "utest.runner.Framework"
  }
}
