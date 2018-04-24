import java.util

import datasource.kafka.KafkaSource
import org.apache.hadoop.util.ClassUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import dataingest.process.DataIngestProcessor

/**
  * Created by dingbingbing on 4/23/18.
  */
object DataIngestDriver {
  val usage =
    s"""
       |Usage: DataIngestDriver -Dkafka.brokers=<brokers> -Dkafka.topics=<topics>
       |  -D<spark.streaming.batchms>=<batch duration in milliseconds>
       |  -D<other key>=<others value>
       |
       |  <kafka.brokers> is a list of one or more Kafka brokers
       |  <kafka.topics>  one topic to consume from
       |
        """.stripMargin

  def main(args: Array[String]) {
    StreamingLogUtils.setStreamingLogLevels()

    val (hadoopParameters, brokers, topics, batchDuration) = getJobParameters(args)

    val sparkConf = new SparkConf().setAppName("BigData-Ingest")
      .setJars(Seq(ClassUtil.findContainingJar(getClass)))
      .set("spark.streaming.kafka.maxRetries", "3")

    var zkCkPath = "/spark-gateway/checkpoint"

    if (hadoopParameters.get("isLocal").equals("true")) {
      sparkConf.setMaster("local[1]")
        .set("spark.ui.enabled", "false")
        .set("spark.logLineage", "true")
      zkCkPath = zkCkPath + "_local"
    }

    // stream context with x batch duration
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchDuration))
    ssc.sparkContext.setCheckpointDir("checkpoint-ipo-11/")

    val messages = KafkaSource.kafkaStream(ssc, brokers,
      hadoopParameters.get("hbase.zookeeper.quorum"),
      zkCkPath,
      topics,
      false)

    //messages.print()
    //PanoramaTransactionProcessor.process(ssc, hadoopParameters, messages)
    DataIngestProcessor.process(ssc, hadoopParameters, messages)

    ssc.start()
    ssc.awaitTermination()
  }

  def getJobParameters(args: Array[String]): (util.HashMap[String, String], String, String, Int) = {
    val hadoopParameters = new util.HashMap[String, String]
    for (arg <- args) {
      if (arg.startsWith("-D")) {
        val argKV = arg.substring(2)
        val argKVsp = argKV.split("=")
        hadoopParameters.put(argKVsp(0), argKVsp(1))
      }
    }
    println("hadoop style args:" + hadoopParameters)

    val brokers: String = hadoopParameters.get("kafka.brokers")
    val topics: String = hadoopParameters.get("kafka.topics")
    if (brokers == null || topics == null) {
      System.err.println(usage)
      System.exit(1)
    }

    val batchDuration = {
      if (hadoopParameters.containsKey("spark.streaming.batchms"))
        hadoopParameters.get("spark.streaming.batchms").toInt
      else
        4000
    }
    (hadoopParameters, brokers, topics, batchDuration)
  }
}
