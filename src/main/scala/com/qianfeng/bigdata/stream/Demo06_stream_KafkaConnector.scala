package com.qianfeng.bigdata.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

/**
  * 使用flink-kafka的连接器消费kafka的数据
  */
object Demo06_stream_KafkaConnector {
    def main(args: Array[String]): Unit = {
        //获取执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.enableCheckpointing(10) //开启checkpoint
        //定义kafka消费所需
        val fromTopic = "yq_report"
        val pro: Properties = new Properties()
        //使用类加载器加载consumer，properties
        pro.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))

        //添加flink kafka的消费者
        val flinkKafkaConsumer = new FlinkKafkaConsumer09[String](fromTopic, new SimpleStringSchema(), pro)
        //设置消费者相关信息
        flinkKafkaConsumer.setStartFromEarliest() //从最早的位置开始消费
        //flinkKafkaConsumer.setStartFromLatest() //从最新的位置消费
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true) //设置offset提交基于checkpoint
        val res: DataStreamSource[String] = env.addSource(flinkKafkaConsumer)

        res.print("flink-kafka-")
        //触发执行
        env.execute("kafka connector-")

    }
}
