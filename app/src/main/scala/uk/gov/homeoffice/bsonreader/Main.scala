package uk.gov.homeoffice.bsonreader

import cats.data.*
import cats.effect.*
import com.typesafe.config.*

import java.io.*
import fs2.io.file.{Files => FS2Files, Path}
import java.nio.file.{Files => NioFiles, Paths}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.bson.*
import org.bson.conversions.*
import cats.effect.implicits.{*, given}
import cats.effect.unsafe.implicits.global
import cats.implicits.{*, given}

import com.mongodb.*
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
    val inputStream = filename.endsWith(".gz") match {
      case true => new BufferedInputStream(GZIPInputStream(new BufferedInputStream(new FileInputStream(filename))))
      case false => new BufferedInputStream(new FileInputStream(filename))
    }

    fs2.Stream.repeatEval { nextBsonObject(inputStream) }.unNoneTerminate
      .collect { case Right(json) => json }
  }

  def jsonWriter(filename :String) :OutputStream = {
    filename.endsWith(".gz") match {
      case true => new GZIPOutputStream(new FileOutputStream(filename))
      case false => new FileOutputStream(filename)
    }
  }

  def writeNormalJson(fos: OutputStream, json :Json, idx :Long) :Unit = {
    if (idx > 0) {
      fos.write(",\n".getBytes)
    }
    fos.write(json.spaces4.getBytes)
  }

  def writePandasJson(fos :OutputStream, json :Json, idx :Long) :Unit = {
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

    if (args.contains("--help") || args.contains("-h")) {
      println("bson to json convertor")
      println("java -jar bson-to-json.jar [--pandas] <input-file> <output-file>")
      println("    --pandas: flattens nested json into single depth json array so { 'hello' : { 'thing' : 4 }} becomes { 'hello.thing' : 4 }")
      println("If the input file ends .gz then the file will be run through gzip decompression")
      println("If the output file ends .json then the file will be json encoded, it if ends .csv it will be csv encoded")
      println("If the output file ends .gz then the file will be compressed through gzip decompression")
      println("    -h or --help: print help message and exit")
      return IO(ExitCode.Success)
    } else if (args.length < 2) {
      println("bson to json convertor")
      println(s"expected two arguments, got ${args.length}")
      return IO(ExitCode.Success)
    }

    val inputFile = args.filterNot(_.startsWith("--")).lift(0).getOrElse("")
    val outputFile = args.filterNot(_.startsWith("--")).lift(1).getOrElse(inputFile match {
      case s if s.endsWith(".bson.gz") => s.slice(0, s.length - 8) + ".json.gz"
      case s if s.endsWith(".bson") => s.slice(0, s.length - 5) + ".json"
      case _ => inputFile + ".json"
    })

    val pandasMode = args.contains("--pandas")

    println(s"\nArguments read:\nInput file: $inputFile\nOutput file: $outputFile\nPandas Mode: $pandasMode\n")

    if (!NioFiles.exists(Paths.get(inputFile))) {
      println(s"Input file does not exist: $inputFile")
      sys.exit(1)
    }

    val writer = jsonWriter(outputFile)

    if (!pandasMode) { writer.write("[\n".getBytes) }

    val stringifier = if (pandasMode) writePandasJson else writeNormalJson
    val flattener = if (pandasMode) jsonFlatten else noOpFlatten

    bsonReader(inputFile)
      .zipWithIndex
      .evalTap { (_, idx) => IO(if (idx % 100000 == 0) { println(s"Written $idx") }) }
      .map { (json, idx) => (flattener(json), idx) }
      .map { (json, idx) => (stringifier(writer, json, idx), idx) }
      .map { (_, idx) => idx }
      .compile
      .last
      .map { lastIdx =>
        if (!pandasMode) { writer.write("]".getBytes) }
        println(s"Written ${lastIdx.getOrElse(0)}. Finished")
        writer.close()
      }
      .as(ExitCode(0))
  }

