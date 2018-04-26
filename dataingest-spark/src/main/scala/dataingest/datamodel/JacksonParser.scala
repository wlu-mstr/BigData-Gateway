package dataingest.datamodel

import java.io.{ByteArrayOutputStream, Closeable}

import com.fasterxml.jackson.core._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.json.JSONOptions
import org.apache.spark.sql.types.{ArrayBasedMapData => _, _}
import org.apache.spark.unsafe.types.UTF8String


import scala.collection.mutable.ArrayBuffer

class SparkSQLJsonProcessingException(msg: String) extends RuntimeException(msg)


object JacksonParser {

  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  def parse(input: String,
             schema: StructType,
             columnNameOfCorruptRecords: String = "_corrupt_record",
             configOptions: JSONOptions = JSONOptions.createFromConfigMap(Map.empty)): InternalRow = {

    parseJson(input, schema, columnNameOfCorruptRecords, configOptions)

  }

  /**
    * Parse the current token (and related children) according to a desired schema
    */
  def convertField(factory: JsonFactory,
                    parser: JsonParser,
                    schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

      case (VALUE_STRING, StringType) =>
        parser.getText

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, BinaryType) =>
        parser.getBinaryValue

      case (VALUE_STRING, DateType) =>
        val stringValue = parser.getText
        if (stringValue.contains("-")) {
          // The format of this string will probably be "yyyy-mm-dd".
          DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(parser.getText).getTime)
        } else {
          stringValue.toInt
        }

      case (VALUE_STRING, TimestampType) =>
        DateTimeUtils.stringToTime(parser.getText).getTime

      case (VALUE_NUMBER_INT, TimestampType) =>
        parser.getLongValue

      case (_, StringType) =>
        val writer = new ByteArrayOutputStream()
        tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
          generator => generator.copyCurrentStructure(parser)
        }
        UTF8String.fromBytes(writer.toByteArray)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_STRING, FloatType) =>
        // Special case handling for NaN and Infinity.
        val value = parser.getText
        val lowerCaseValue = value.toLowerCase()
        if (lowerCaseValue.equals("nan") ||
          lowerCaseValue.equals("infinity") ||
          lowerCaseValue.equals("-infinity") ||
          lowerCaseValue.equals("inf") ||
          lowerCaseValue.equals("-inf")) {
          value.toFloat
        } else {
          throw new SparkSQLJsonProcessingException(s"Cannot parse $value as FloatType.")
        }

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

      case (VALUE_STRING, DoubleType) =>
        // Special case handling for NaN and Infinity.
        val value = parser.getText
        val lowerCaseValue = value.toLowerCase()
        if (lowerCaseValue.equals("nan") ||
          lowerCaseValue.equals("infinity") ||
          lowerCaseValue.equals("-infinity") ||
          lowerCaseValue.equals("inf") ||
          lowerCaseValue.equals("-inf")) {
          value.toDouble
        } else {
          throw new SparkSQLJsonProcessingException(s"Cannot parse $value as DoubleType.")
        }

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, dt: DecimalType) =>
        Decimal(parser.getDecimalValue, dt.precision, dt.scale)

      case (VALUE_NUMBER_INT, ByteType) =>
        parser.getByteValue

      case (VALUE_NUMBER_INT, ShortType) =>
        parser.getShortValue

      case (VALUE_NUMBER_INT, IntegerType) =>
        parser.getIntValue

      case (VALUE_NUMBER_INT, LongType) =>
        parser.getLongValue

      case (VALUE_TRUE, BooleanType) =>
        true

      case (VALUE_FALSE, BooleanType) =>
        false

      case (START_OBJECT, st: StructType) =>
        convertObject(factory, parser, st)

      case (START_ARRAY, st: StructType) =>
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        convertArray(factory, parser, st)

      case (START_ARRAY, ArrayType(st, _)) =>
        convertArray(factory, parser, st)

      case (START_OBJECT, ArrayType(st, _)) =>
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) =>
        convertField(factory, parser, udt.sqlType)

      case (token, dataType) =>
        // We cannot parse this token based on the given data type. So, we throw a
        // SparkSQLJsonProcessingException and this exception will be caught by
        // parseJson method.
        throw new SparkSQLJsonProcessingException(
          s"Failed to parse a value for data type $dataType (current token: $token).")
    }
  }

  private def getSchemaFieldIndex(schema: StructType, fieldName: String): Option[Int] = {
    try {
      Some(schema.fieldIndex(fieldName))
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Parse an object from the token stream into a new Row representing the schema.
    *
    * Fields in the json that are not defined in the requested schema will be dropped.
    */
  private def convertObject(factory: JsonFactory,
                            parser: JsonParser,
                            schema: StructType): InternalRow = {
    val row = new GenericMutableRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      getSchemaFieldIndex(schema, parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType))

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
    * Parse an object as a Map, preserving all fields
    */
  private def convertMap(factory: JsonFactory,
                         parser: JsonParser,
                         valueType: DataType): org.apache.spark.sql.catalyst.util.MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += convertField(factory, parser, valueType)
    }
    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  private def convertArray(factory: JsonFactory,
                           parser: JsonParser,
                           elementType: DataType): org.apache.spark.sql.catalyst.util.ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += convertField(factory, parser, elementType)
    }

    new GenericArrayData(values.toArray)
  }

  def parseJson(input: String,
                        schema: StructType,
                        columnNameOfCorruptRecords: String ,
                        configOptions: JSONOptions): InternalRow = {

    def failedRecord(record: String): InternalRow = {
      null
    }

    val factory = new JsonFactory()
    configOptions.setJacksonOptions(factory)


    val record = input

    if (record.trim.isEmpty) {
      null
    } else {
      try {
        tryWithResource(factory.createParser(record)) { parser =>
          parser.nextToken()

          convertField(factory, parser, schema) match {
            case null => failedRecord(record)
            case row: InternalRow => row
            case _ => failedRecord(record)
          }
        }
      } catch {
        case _: JsonProcessingException =>
          failedRecord(record)
        case _: SparkSQLJsonProcessingException =>
          failedRecord(record)
      }
    }

  }
}
