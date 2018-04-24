package dataingest.configs

import dataingest.datamodel.Data
import org.apache.spark.sql.types.StructType

/**
  * Created by dingbingbing on 4/23/18.
  */
case class SchemaConfig(appKey: String,
                        eventType: String,
                        eventSchema: StructType) {
  def parseData(message:String, key:String): Data ={
    null
  }
}
