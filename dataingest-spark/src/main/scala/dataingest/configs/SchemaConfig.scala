package dataingest.configs

import dataingest.datamodel.JacksonParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
  * Created by dingbingbing on 4/23/18.
  */
case class SchemaConfig(appKey: String,
                        eventType: String,
                        eventSchema: StructType) {
  def parseData(message:String, key:String): (String, InternalRow) ={
    (key, JacksonParser.parse(message, eventSchema))
  }
}
