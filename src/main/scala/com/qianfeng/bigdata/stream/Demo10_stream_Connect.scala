package com.qianfeng.bigdata.stream

import com.qianfeng.bigdata.common.TempInfo
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Union和Connect： 合并流
  *
  * union：DataStream* -> DataStream
  * 是将两个或者多个流进行合并，形成新的数据流：union的多个子流的类型需要一直
  *
  * connect：DataStream，DataStream -> ConnectedStream
  * 1.connect只能连接两个Stream
  * 2.被连接的两个子流的类型可以不一致
  * 3.两个被连接的流之间可以进行状态的数据的共享，一个流的结果会影响另外一个流，通常用于做累加非常有用
  *
  */
object Demo10_stream_Connect {

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
        val common: DataStream[TempInfo] = splitStream.select("正常")
        val execption: DataStream[TempInfo] = splitStream.select("异常")

        //使用union进行合并
        val connectedStream: ConnectedStreams[TempInfo, TempInfo] = common.connect(execption)
        connectedStream.map(
            common1 =>("正常旅客id：" + common1.uid + "姓名：" + common1.uname),
            execption1 => ("异常旅客id：" + execption1.uid + "姓名：" + execption1.uname + "温度：" + execption1.temp)
        ).print("connected1 ---")

        val c1: DataStream[(Int, String)] = splitStream.select("正常").map(person => {
            (person.uid, person.uname)
        })

        val e1: DataStream[(Int, String, Double)] = splitStream.select("异常").map(person => {
            (person.uid, person.uname, person.temp)
        })

        //connect连接的两个流可以是类型不一致的
        val res1: ConnectedStreams[(Int, String), (Int, String, Double)] = c1.connect(e1)

        res1.map(
            common1 =>("正常旅客id：" + common1._1 + "姓名：" + common1._2),
            execption1 => ("异常旅客id：" + execption1._1 + "姓名：" + execption1._2 + "温度：" + execption1._3)
        ).print("connected2 ---")


        env.execute("union")
    }
}
