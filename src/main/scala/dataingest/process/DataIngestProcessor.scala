package dataingest.process

import java.util

import dataingest.configs.SchemaConfig
import dataingest.datamodel.{Data, DataKey}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SQLContext


/**
  * Created by dingbingbing on 4/23/18.
  */
object DataIngestProcessor {
  def convertToMessage(itr: scala.Iterator[(String, String)],
                       broadcastConfigs: Broadcast[Seq[SchemaConfig]]): scala.Iterator[Data] = {
    val amConfigs = broadcastConfigs.value
    val res = ArrayBuffer[Data]()
    itr.foreach(
      m => {
        for (config <- amConfigs) {
          val log = config.parseData(m._2, m._1)
          if (log != null) {
            res.append(log)
          }
        }
      }
    )
    res.iterator
  }

  def process(streamingContext: StreamingContext,
              hadoopParameters: util.HashMap[String, String],
              rawLogStream: InputDStream[(String, String)]): Unit = {
    rawLogStream.foreachRDD(
      rdd => {
        val sqlSc = new SQLContext(streamingContext.sparkContext)
        import sqlSc.implicits._
        val msgConfigs = null
        val rawJsonMsg:RDD[Data] = rdd.mapPartitions(itr => convertToMessage(itr, msgConfigs))
        //val groups: RDD[(DataKey, Iterable[Data])] = rawJsonMsg.groupBy(Data.getKey)
        rawJsonMsg.toDF().write.partitionBy("")



      }
    )
  }
}
