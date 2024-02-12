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

  def writeNormalJson(fos: FileOutputStream, json :Json) :Unit = {
    fos.write(json.spaces4.getBytes)
    fos.write(",\n".getBytes)
  }

  def writePandasJson(fos :FileOutputStream, json :Json) :Unit = {
    fos.write(json.noSpaces.getBytes)
    fos.write("\n".getBytes)
  }

  // stolen from https://stackoverflow.com/questions/58026172/flattening-nested-json-objects-with-circe
  def jsonFlatten(json :Json) :Json = {
    def flattenDeep(combineKeys: (String, String) => String)(value: Json): Json = {
      def flattenToFields(value: Json): Option[Iterable[(String, Json)]] = {
        value.fold(
          jsonNull = None,
          jsonNumber = _ => None,
          jsonString = _ => None,
          jsonBoolean = _ => None,
          jsonObject = { obj =>
            val fields = obj.toIterable.flatMap {
              case (field, json) =>
                flattenToFields(json).fold(Iterable(field -> json)) {
                  _.map {
                    case (innerField, innerJson) =>
                      combineKeys(field, innerField) -> innerJson
                  }
                }
            }
            Some(fields)
          },
          jsonArray = { array =>
            val fields = array.zipWithIndex.flatMap {
              case (json, index) =>
                flattenToFields(json).fold(Iterable(index.toString -> json)) {
                  _.map {
                    case (innerField, innerJson) =>
                      combineKeys(index.toString, innerField) -> innerJson
                  }
                }
            }
            Some(fields)
          }
        )
      }
      flattenToFields(value).fold(value)(Json.fromFields)
    }

    def dotNotation(a :String, b :String) = s"$a.$b"
    flattenDeep(dotNotation)(json)
  }

  def noOpFlatten(json :Json) :Json = json

  def run(args: List[String]): IO[ExitCode] = {
    val inputFile = args(0)
    val outputFile = args(1)
    val pandasMode = args.contains("--pandas")

    val fos = new FileOutputStream(outputFile)

    if (pandasMode) { fos.write("[\n".getBytes) }

    val stringifier = if (pandasMode) writePandasJson else writeNormalJson
    val flattener = if (pandasMode) jsonFlatten else noOpFlatten

    bsonReader(inputFile)
      .zipWithIndex
      .evalTap { (_, idx) => IO(if (idx % 100000 == 0) { println(s"Written $idx") }) }
      .map { (json, _) => flattener(json) }
      .map { json => stringifier(fos, json) }
      .compile
      .drain
      .map { _ =>
        if (!pandasMode) { fos.write("]".getBytes) }
        fos.close()
      }
      .as(ExitCode(0))
  }

