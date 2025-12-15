package build
import mill.*, scalalib.*

object app extends ScalaModule {
  def scalaVersion = "3.7.4"

  def mvnDeps = Seq(
    mvn"com.lihaoyi::scalatags:0.12.0",
    mvn"com.typesafe:config:1.4.3",
    mvn"co.fs2::fs2-core:3.9.3",
    mvn"co.fs2::fs2-io:3.9.3",
    mvn"org.apache.commons:commons-compress:1.25.0",
    mvn"commons-codec:commons-codec:1.16.0",
    mvn"com.typesafe.scala-logging::scala-logging:3.9.4",
    mvn"ch.qos.logback:logback-classic:1.2.3",
    mvn"org.typelevel::log4cats-slf4j:2.6.0".withDottyCompat(scalaVersion()),
    mvn"org.mongodb.scala::mongo-scala-driver:4.4.0".withDottyCompat(scalaVersion()),
    mvn"io.circe::circe-core:0.14.1",
    mvn"io.circe::circe-generic:0.14.1",
    mvn"io.circe::circe-parser:0.14.1",
    mvn"com.github.scopt::scopt:4.1.0"
  )

  object test extends ScalaTests, TestModule.Utest {
    def utestVersion = "0.9.1"
  }

}

