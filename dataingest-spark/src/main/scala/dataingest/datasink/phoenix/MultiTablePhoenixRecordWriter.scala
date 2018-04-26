package dataingest.datasink.phoenix

import java.sql.{Connection, PreparedStatement, SQLException}

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.phoenix.mapreduce.PhoenixOutputCommitter
import org.apache.phoenix.mapreduce.util.{ConnectionUtil, PhoenixConfigurationUtil}
import org.apache.phoenix.util.{ColumnInfo, QueryUtil}

import scala.collection.mutable
import collection.JavaConversions._


class MultiTablePhoenixRecordWriter(configuration: Configuration) extends RecordWriter[NullWritable, MultiTalbePhoenixRecordWritable] {
  val LOG: Log = LogFactory.getLog(classOf[MultiTablePhoenixRecordWriter])

  val connection: Connection = ConnectionUtil.getOutputConnection(configuration)
  val batchSize: Long = PhoenixConfigurationUtil.getBatchSize(configuration)
  var numRecords: mutable.Map[String, Long] = mutable.Map[String, Long]()
  var upsertStatements: mutable.Map[String, PreparedStatement] = mutable.Map[String, PreparedStatement]()

  def getPreparedStatement(tableName: String, colmInfos: List[ColumnInfo]): PreparedStatement = {
    val statement = upsertStatements.get(tableName)
    if (statement.nonEmpty) {
      return statement.get
    }
    val upsertSql = configuration.get(Util.upsertStatementKey(tableName), QueryUtil.constructUpsertStatement(tableName, colmInfos))


    val newStatement = connection.prepareStatement(upsertSql)
    upsertStatements(tableName) = newStatement

    newStatement

  }

  override def write(key: NullWritable, value: MultiTalbePhoenixRecordWritable): Unit = {
    val tableName = value.getTableName
    val preparedStatement = getPreparedStatement(tableName, value.columnMetaDataList)
    if (preparedStatement == null) {
      LOG.error(s"Can't find upsert statement for table $tableName")
      return
    }

    value.write(preparedStatement)

    val currNum: Long = numRecords.getOrElse(tableName, 0L) + 1L

    println(preparedStatement)
    preparedStatement.execute()
    if (currNum % batchSize == 0) {
      connection.commit()
    }

    numRecords(tableName) = currNum
  }

  override def close(context: TaskAttemptContext): Unit = {
    try {
      connection.commit()
    } catch {
      case e: SQLException => LOG.error("Exception during database commit " + e.toString)
      case _ => LOG.error("Exception during database commit ")
    } finally {

      connection.close()
    }
  }
}

class MultiTablePhoenixOutputFormat extends OutputFormat[NullWritable, MultiTalbePhoenixRecordWritable] {
  override def checkOutputSpecs(context: JobContext): Unit = {
    // we can't know ahead of time if it's going to blow up when the user
    // passes a table name that doesn't exist, so nothing useful here.
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    new PhoenixOutputCommitter()
  }

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[NullWritable, MultiTalbePhoenixRecordWritable] = {
    new MultiTablePhoenixRecordWriter(context.getConfiguration)
  }


}