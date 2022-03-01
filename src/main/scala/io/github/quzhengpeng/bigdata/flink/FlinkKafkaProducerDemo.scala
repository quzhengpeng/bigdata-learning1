package io.github.quzhengpeng.bigdata.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

// FlinkKafkaConsumerDemo
object FlinkKafkaProducerDemo {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "GROUP_1"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 开启检查点，每1秒提交一次，且需要事先有且只有一次的消息消费
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new FlinkKafkaConsumer[String]("kafka_test", new SimpleStringSchema(), kafkaProps)
    consumer.setStartFromEarliest()

    val stream = env.addSource(consumer)
    val new_stream = stream.filter(x => x.contains("10")).map(x => {
      val sp = x.split(" ")

//      print(sp(3))
//      (sp.mkString("==="))
      sp(5)
    })
    new_stream.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
