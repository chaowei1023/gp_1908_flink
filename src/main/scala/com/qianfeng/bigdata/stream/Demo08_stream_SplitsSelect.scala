package com.qianfeng.bigdata.stream

import com.qianfeng.bigdata.common.{TempInfo, WordCount}
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Split/Select
  * Split：DataStream -> SplitStream
  * 拆分流，也就是将一个DataStream拆分成多个SplitStream
  *
  * Select：SplitStream -> DataStream
  * 跟Split搭配使用，先拆分，然后在选择对应的流
  *
  * 场景：
  * 1.有多重类型的数据，并多重数据的计算方式不一样
  * 2.正常和异常数据的拆分
  */
object Demo08_stream_SplitsSelect {

    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dStream: DataStream[String] = env.socketTextStream("master", 6666)

        //输入数据格式
        val splitStream: SplitStream[TempInfo] = dStream.map(personInfo => {
            val fields: Array[String] = personInfo.split(" ")

            TempInfo(fields(0).toInt, fields(1).toString.trim, fields(2).toDouble, fields(3).toLong, fields(4).toString.trim)
        })
            .split((temp: TempInfo) => if (temp.temp >= 36.0 && temp.temp <= 37.8) Seq("正常") else Seq("异常"))

        //select使用选择流
        splitStream.select("正常").print("正常旅客")
        splitStream.select("异常").print("异常旅客")

        env.execute("spilt select")
    }
}
