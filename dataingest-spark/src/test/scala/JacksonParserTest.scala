import org.apache.spark.sql.types.{DataType, StructField, StructType}
import dataingest.datamodel.JacksonParser
/**
  * Created by dingbingbing on 4/24/18.
  */
object JacksonParserTest extends App {

  val configSchema = List(
    ("eventid", "\"long\""),
    ("eventtime", "\"timestamp\""),
    ("eventtype", "\"string\""),
    ("name", "\"string\""),
    ("birth", "\"timestamp\""),
    ("age", "\"double\""),
    ("event", "{\"type\":\"array\",\"elementType\":\"long\", \"containsNull\":true}")
  )
  val schema:StructType = StructType(configSchema.map{
    cf => StructField(cf._1, DataType.fromJson(cf._2))
  })

  val json = """{"name":"Michael", "eventid":21, "eventtime":"2018-02-01 12:00:00", "eventtype":"tp", "birth":1517457600, "age":10, "event":[1,2,3]}"""

  val row = JacksonParser.parse(json, schema)

  println(row)


}
