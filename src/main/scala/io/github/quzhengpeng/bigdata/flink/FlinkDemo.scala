package io.github.quzhengpeng.bigdata.flink

//import io.github.quzhengpeng.bigdata.flink.FlinkDemo.ReadingFromKafka.{KAFKA_BROKER, TRANSACTION_GROUP, ZOOKEEPER_HOST}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import java.util.Properties

object FlinkDemo {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "GROUP_1"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    val a = env.addSource(new FlinkKafkaConsumer[String]("new", new SimpleStringSchema(), kafkaProps))
//    val transaction = env
//      .addSource(
//        new FlinkKafkaConsumer[String]("new", new SimpleStringSchema(), kafkaProps)
//      )
//    transaction.print()
//    env.execute()

    //        val env1: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //        StreamExecutionEnvironment.getExecutionEnvironment
    //
    //        val lines: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String]("topic", new SimpleStringSchema(), properties))

//    val lines = env.addSource(new FlinkKafkaConsumer[String a]())
  }



}
