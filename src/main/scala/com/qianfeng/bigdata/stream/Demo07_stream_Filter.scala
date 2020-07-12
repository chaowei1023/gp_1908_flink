package com.qianfeng.bigdata.stream


import com.qianfeng.bigdata.common.WordCount
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * flatMap：扁平化需要，，，订单有多个商品 订单id 商品id
  */
object Demo07_stream_Filter {
    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dStream: DataStream[String] = env.socketTextStream("master", 6666)

        val res: DataStream[WordCount] = dStream.flatMap(_.split(" "))
            .filter(_.length > 5)
            .map(new WordCount(_, 1))
            .keyBy("word")
            .sum("count")

        res.print("filter-")


        env.execute("Demo07_stream_Filter")
    }
}
