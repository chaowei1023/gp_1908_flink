package com.qianfeng.bigdata.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


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
  * 2、将如上计算结果打入到Redsi中
  */
object Demo17_stream_RedisSink {
    def main(args: Array[String]): Unit = {

        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dstream: DataStream[String] = env.socketTextStream("master", 6666)

        val res: DataStream[(String, String)] = dstream.map(line => {
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
                (y._1, (y._2._1 + "_" + y._2._2))
            })

        res.print("yq--")

       //将res结果打入Redis中 --
        val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("master")
            .setPort(6379)
            .build()

        //获取redis的sink
        val myRedisSink = new RedisSink(config, new MyRedisMapper)

        res.addSink(myRedisSink)

        env.execute("kafkaSink")
    }
}

/**
  * 由于Redis是一个KV类型的存储，所以泛型最好是一个k-v类型
  *
  * key：时间和省份维度组合
  * value：新增等
  */
class MyRedisMapper extends RedisMapper[(String, String)] {
    //redis的命令描述器 --set还是hset
    //1.additionalKey，当我们存储为Hash或者SortedSet必须要设置额外的key
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.SET, null)
    }

    //存储到redis中的key
    override def getValueFromData(data: (String, String)): String = {
        data._2
    }

    //存储到redis中的value
    override def getKeyFromData(data: (String, String)): String = {

        data._1
    }
}
