package dataingest.datasink.phoenix

/**
  * Created by dingbingbing on 4/24/18.
  */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.spark.PhoenixRecordWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._


class MultiTableProductRDDFunctions[V <: InternalRow](data: RDD[(String, V)]) extends Serializable {

  def saveToPhoenix(conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None,
                    tenantId: Option[String] = None): Unit = {

    val url = zkUrl.get
    conf.set("hbase.zookeeper.quorum", url)

    val phxRDD = data.mapPartitions { rows =>
      val conf = new Configuration
      conf.set("hbase.zookeeper.quorum", url)

      rows.map { row =>
        val key = row._1
        val rowBody: InternalRow = row._2

        val tableName = Util.getTableName(key, null)
        val structType: StructType = Util.getStructType(key, null)
        val upsertColumns = Util.getUpsertColumns(key, null)

        val columns = Util.getUpsertColumnMetadataList(tableName, upsertColumns, conf).toList

        val rec = new MultiTalbePhoenixRecordWritable(tableName, columns)

        rowBody.toSeq(structType).foreach { e => rec.add(e) }
        (null, rec)
      }
    }

    phxRDD.foreach(println)
    // Save it
    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[MultiTalbePhoenixRecordWritable],
      classOf[MultiTablePhoenixOutputFormat],
      conf
    )
  }
}