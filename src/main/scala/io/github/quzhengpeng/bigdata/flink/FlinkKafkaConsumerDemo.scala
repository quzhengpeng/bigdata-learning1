package io.github.quzhengpeng.bigdata.flink

import java.util.Properties
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

// FlinkKafkaConsumerDemo
object FlinkKafkaConsumerDemo {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "GROUP_1"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 开启检查点，每1秒提交一次，且需要事先有且只有次的消息消费
    env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    val new_stream = stream
      .filter(x => x.contains("10"))
      .map(str => (str.substring(1, 11), str.split(" ")(5).toInt))
      .keyBy(x=>x._1)

//      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
      .max(1)
      .print("f")
//      .max(1)
//      .win
//      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
//      .process()
//      .aggregate(1)

//      .keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
//      .windowAll(Time.seconds(1))
//      .max(0)
//      .print()
//      .countWindowAll(Time.seconds(3))
//      .windowAll(Time.seconds(5), Time.seconds(1))
      //      .timeWindowAll(Time.seconds(1))
//      .sum(0)
    //      .windowAll()
//    new_stream.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
