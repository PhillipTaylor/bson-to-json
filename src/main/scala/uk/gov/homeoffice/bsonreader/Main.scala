package uk.gov.homeoffice.bsonreader

import cats.data.*
import cats.effect.*
import cats.effect.implicits.{*, given}
import cats.effect.unsafe.implicits.global
import cats.implicits.{*, given}

import com.mongodb.*
import com.typesafe.config.*

import java.io.*
import fs2.io.file.{Files => FS2Files, Path}
import java.nio.file.{Files => NioFiles, Paths}
import java.util.zip.GZIPInputStream

import org.bson.*
import org.bson.conversions.*
import org.bson.json.*
import _root_.io.circe.*
import _root_.io.circe.parser.*
import scala.util.*
import scopt.OParser

object MainApp extends IOApp:
  val bd = new BasicBSONDecoder()

  def nextBsonObject(inputStream :InputStream) :IO[Option[Either[String, Json]]] = IO {
    Try(bd.readObject(inputStream)).toEither match {
      case Right(bsonObj) => toJson(bsonObj) match {
        case Right(jsonObj) => Some(Right(jsonObj))
        case Left(err) => Some(Left(err))
      }
      case Left(exc) if Option(exc.getMessage()).isEmpty => None
      case Left(exc) => Some(Left(s"Exception reading bson stream: $exc"))
    }
  }

  def toJson(bsonObject :BSONObject) :Either[String, Json] = {
    val jsonString = BasicDBObject(bsonObject.toMap).toJson
    parse(jsonString).left.map { case ex => ex.getMessage }
  }

  def bsonReader(filename :String) :fs2.Stream[IO, _root_.io.circe.Json] = {
    val inputStream = new BufferedInputStream(GZIPInputStream(new BufferedInputStream(new FileInputStream(filename))))
    fs2.Stream.repeatEval { nextBsonObject(inputStream) }.unNoneTerminate
      .collect { case Right(json) => json }
  }

  def run(args: List[String]): IO[ExitCode] = {
    val inputFile = args(0)
    val outputFile = args(1)
    val fos = new FileOutputStream(outputFile)
    fos.write("[\n".getBytes)
    bsonReader(inputFile)
      .map { json =>
        fos.write(json.spaces4.getBytes)
        fos.write(",\n".getBytes)
      }
      .compile
      .drain
      .map { _ =>
        fos.write("]".getBytes)
        fos.close()
      }
      .as(ExitCode(0))
  }

