package com.qianfeng.bigdata.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


/**
  * Agg聚合
  *keyedStream.sum(0)
  *keyedStream.sum("key")
  *keyedStream.min(0)
  *keyedStream.min("key")
  *keyedStream.max(0)
  *keyedStream.max("key")
  *keyedStream.minBy(0)
  *keyedStream.minBy("key")
  *keyedStream.maxBy(0)
  *keyedStream.maxBy("key")
  */
object Demo12_stream_Agg {
    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 66), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 33), Tuple2(100, 10))

        val keyedStream: KeyedStream[(Int, Int), Tuple] = dstream.keyBy(0)

        //聚合算子
        //keyedStream.sum(1).print("sum --")
        keyedStream.min(1).print("min --")
        keyedStream.minBy(1).print("minBy --")
        //keyedStream.max(1).print("max --")
        //keyedStream.maxBy(1).print("maxBy --")

        env.execute("agg")
    }
}
