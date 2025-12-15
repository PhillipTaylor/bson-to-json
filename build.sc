package build
import mill.*, scalalib.*

object app extends ScalaModule {
  def scalaVersion = "3.7.4"

  def mvnDeps = Seq(
    mvn"com.lihaoyi::scalatags:0.13.1",
    mvn"com.typesafe:config:1.4.5",
    mvn"co.fs2::fs2-core:3.12.2",
    mvn"co.fs2::fs2-io:3.12.2",
    mvn"org.apache.commons:commons-compress:1.28.0",
    mvn"commons-codec:commons-codec:1.20.0",
    mvn"com.typesafe.scala-logging::scala-logging:3.9.6",
    mvn"ch.qos.logback:logback-classic:1.5.22",
    mvn"org.typelevel::log4cats-slf4j:2.7.1".withDottyCompat(scalaVersion()),
    mvn"org.mongodb.scala::mongo-scala-driver:5.6.2".withDottyCompat(scalaVersion()),
    mvn"io.circe::circe-core:0.14.15",
    mvn"io.circe::circe-generic:0.14.15",
    mvn"io.circe::circe-parser:0.14.15",
    mvn"com.github.scopt::scopt:4.1.0"
  )

  object test extends ScalaTests, TestModule.Utest {
    def utestVersion = "0.9.1"
  }

}

