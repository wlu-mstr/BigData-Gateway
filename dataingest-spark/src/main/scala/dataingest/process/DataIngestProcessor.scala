package dataingest.process

import java.util

import dataingest.configs.SchemaConfig
import dataingest.datasink.datasink._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dingbingbing on 4/23/18.
  */
object DataIngestProcessor {
  def convertToMessage(itr: scala.Iterator[(String, String)],
                       broadcastConfigs: Broadcast[SchemaConfig]): scala.Iterator[(String, InternalRow)] = {
    val config = broadcastConfigs.value
    itr.flatMap(m => Some(config.parseData(m._2, m._1)))
  }

  def process(streamingContext: StreamingContext,
              hadoopParameters: util.HashMap[String, String],
              rawLogStream: InputDStream[(String, String)]): Unit = {
    rawLogStream.foreachRDD(
      rdd => {
        // get configs
        val schemaConfig: SchemaConfig = SchemaConfig("app_key", "tracing",
          dataingest.datasink.phoenix.Util.getStructType(null, null))
        val schemaConfigBroadcast = streamingContext.sparkContext.broadcast(schemaConfig)

        val rawJsonMsg:RDD[(String, InternalRow)] = rdd.mapPartitions(
          itr => convertToMessage(itr, schemaConfigBroadcast)
        )

        rawJsonMsg.saveToPhoenix(zkUrl = Some("localhost:2181"))
        rawJsonMsg.foreach(println)


      }
    )
  }
}
