package com.qianfeng.bigdata.day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Test_ReadSocket {
    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dStream: DataStream[String] = env.socketTextStream("master", 6666)

        dStream.print("socket ---")

        env.execute("test read socket")
    }
}
