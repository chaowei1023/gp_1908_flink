package com.qianfeng.bigdata.day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.api.scala._
/**
  * 实时统计的WD -scala
  * 注意：
  * 本地跑flink实时的时候，我是用自带的flink的streaming依赖会报：
  *     StreamExecutionEnvironment 类找不到
  *
  * 解决办法：
  *     引入flink安装包下的lib下的所有的jar包进行编译即可。
  *     引入流程：
  *     project structure-->model-->dependecy-->add jar.. -->flink本地lib目录
  */
object Demo03_Stream_WC {
    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dStream: DataStream[String] = env.socketTextStream("master", 6666)

        //3.基于source的transformation
        //具体转换
        val sumed: DataStream[(String, Int)] = dStream.flatMap(_.split("\\s+"))
            .map((_, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(5), Time.seconds(5))
            .sum(1)

        //4.结果sink
        sumed.print()

        //5.触发执行
        //env.execute("stream wc scala")



    }
}
