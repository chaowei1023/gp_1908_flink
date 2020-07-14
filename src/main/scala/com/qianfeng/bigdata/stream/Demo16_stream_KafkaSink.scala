package com.qianfeng.bigdata.stream


import java.lang
import java.util.Properties

import com.qianfeng.bigdata.common.YQ
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer09, KafkaSerializationSchema}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
  *
  * 原始数据：
  * date province add possible
  * 2020-7-1 beijing 1 2
  * 2020-7-2 beijing 2 1
  * 2020-7-3 beijing 1 0
  * 2020-7-3 tianjin 2 1
  *
  * 需求：
  * 1、算出每天、省份的adds、possible
  * 2、将如上计算结果打入到kafka中
  * 3、如果重启服务希望对已有的key进行累加，不是重算-----未做
  */
object Demo16_stream_KafkaSink {
    def main(args: Array[String]): Unit = {

        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dstream: DataStream[String] = env.socketTextStream("master", 6666)

        val res: DataStream[YQ] = dstream.map(line => {
            val fields: Array[String] = line.split(" ")
            val date: String = fields(0).trim
            val province: String = fields(1).trim
            val add: Int = fields(2).trim.toInt
            val possible: Int = fields(3).trim.toInt
            (date + "_" + province, (add, possible))
        })
            .keyBy(0)
            .reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
            .map(y => {
                val date_province: Array[String] = y._1.split("_")
                new YQ(date_province(0), date_province(1), y._2._1, y._2._2)
            })

        res.print("yq--")

       //将res结果打入kafka中 -- broker_list totopic 序列化
        val broker_list = "master:9092,slave1:9092,slave2:9092"
        val pro = new Properties()
        pro.put("bootstrap.servers", broker_list)
        val to_topic = "yq_report"

        val flinkKafkaProduce: FlinkKafkaProducer09[YQ] = new FlinkKafkaProducer09[YQ](
            to_topic,
            new YQSchema(to_topic),
            pro
        )

        res.addSink(flinkKafkaProduce)

        env.execute("kafkaSink")
    }
}

//自定义kafka的生产序列化
class YQSchema(topic: String) extends KeyedSerializationSchema[YQ] {

    /*
    //序列化
    override def serialize(t: YQ, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        //从t中取出所有需要打入到kafka中的值
        val date: String = t.date
        val province: String = t.province
        val add: Int = t.add
        val possible: Int = t.possible
        //封装返回
        new ProducerRecord[Array[Byte], Array[Byte]](topic, (date + "_" +  province + "_" + add + "_" + possible).getBytes)

    }
    */
    override def serializeKey(element: YQ): Array[Byte] = {
        null
    }

    override def getTargetTopic(element: YQ): String = {
        topic
    }

    override def serializeValue(element: YQ): Array[Byte] = {
        val date: String = element.date
        val province: String = element.province
        val add: Int = element.add
        val possible: Int = element.possible
        (date + "_" +  province + "_" + add + "_" + possible).getBytes
    }
}
