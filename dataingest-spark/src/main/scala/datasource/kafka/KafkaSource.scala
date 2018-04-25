package datasource.kafka

/**
  * Created by dingbingbing on 4/23/18.
  */
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

object KafkaSource {

  val logger = LoggerFactory.getLogger(KafkaSource.getClass)

  def kafkaStream(ssc: StreamingContext, brokers: String, zkHosts: String, zkPath: String,
                  topic: String): InputDStream[(String, String)] = {
    kafkaStream(ssc, brokers, zkHosts, zkPath, topic, true)
  }
  // Kafka input stream
  def kafkaStream(ssc: StreamingContext, brokers: String, zkHosts: String, zkPath: String,
                  topic: String, cache: Boolean): InputDStream[(String, String)] = {
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokers)
    val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer.getInstance())
    val storedOffsets = readFromOffsets(zkClient, zkPath, topic)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
      case Some(fromOffsets) =>
        // start from previously saved offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
          (String, String)](ssc, kafkaParams, fromOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }

    if (cache) {
      kafkaStream.cache()
    }
    // save the offsets
    kafkaStream.foreachRDD(rdd => saveFromOffsets(zkClient, zkPath, rdd))

    kafkaStream
  }

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
      (Some(client.readData(path, stat)), stat)
    } catch {
      case e: ZkNoNodeException =>
        (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
    }
  }
  // Read the previously saved offsets from Zookeeper
  private def readFromOffsets(zkClient: ZkClient, zkPath: String, topic: String): Option[Map[TopicAndPartition, Long]] = {
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()

    val (offsetsRangesStrOpt, _) = readDataMaybeNull(zkClient, zkPath)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.debug(s"Read offset ranges: $offsetsRangesStr")

        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
          .toMap

        logger.info("Done reading offsets from Zookeeper. Took " + stopwatch)

        Some(offsets)
      case None =>
        logger.info("No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }

  // Save the offsets back to Zookeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  private def saveFromOffsets(zkClient: ZkClient, zkPath: String, rdd: RDD[_]): Unit = {
    logger.info("Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => logger.debug(s"Using $offsetRange"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.debug(s"Writing offsets to Zookeeper: $offsetsRangesStr")
    updatePersistentPath(zkClient, zkPath, offsetsRangesStr)

    logger.info("Done updating offsets in Zookeeper. Took " + stopwatch)
  }

  // very simple stop watch to avoid using Guava's one
  class Stopwatch {
    private val start = System.currentTimeMillis()

    override def toString = (System.currentTimeMillis() - start) + " ms"
  }

}