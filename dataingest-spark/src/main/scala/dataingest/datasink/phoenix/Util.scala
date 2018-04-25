package dataingest.datasink.phoenix

import java.sql.SQLException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.mapreduce.util.{ConnectionUtil, PhoenixConfigurationUtil}
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Created by WLU on 4/24/2018.
  */
object Util {
  val UPSERT_STATEMENT_KEY_PREFIX = "UPSERT.STATEMENT.PREFIX."

  def upsertStatementKey(tableName: String): String = UPSERT_STATEMENT_KEY_PREFIX + tableName.toUpperCase()

  def getTableName(key: String, conf: Configuration): String = {
    // TODO
    "eventlog"
  }

  def getStructType(key: String, conf: Configuration): StructType = {
    // TODO
    val configSchema = List(
      ("eventid", "integer"),
      ("eventtime", "timestamp"),
      ("eventtype", "string"),
      ("name", "string"),
      ("birth", "timestamp"),
      ("age", "integer")
    )
    StructType(configSchema.map {
      cf => StructField(cf._1, DataType.fromJson("\"" + cf._2 + "\""))
    })
  }

  def getUpsertColumns(key: String, conf: Configuration): List[String] = {
    // TODO
    List[String]("eventid", "eventtime", "eventtype", "name", "birth", "age")
  }

  @throws[SQLException]
  def getUpsertColumnMetadataList(tableName: String,
                                  upsertColumnList: List[String],
                                  configuration: Configuration): util.List[ColumnInfo] = {
    val connection = ConnectionUtil.getOutputConnection(configuration)
    val upsertColumnList = PhoenixConfigurationUtil.getUpsertColumnNames(configuration)

    val columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, upsertColumnList)

    connection.close()
    columnMetadataList
  }
}