package com.qianfeng.bigdata.stream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * KeyBy：DataStream -> KeyedStream
  * 将所有数据记录中相同的key分配到同一个分区中去，内部使用散列分区去实现；
  * 类似于sql中的group by
  * 支持位置，支持字段，支持数组，支持POJO（对象）
  * keyby通常和reduce|window搭配使用
  *
  * Reduce：聚合合并  KeyedStream -> DataStream
  * 将每个分区中数据进行聚合计算，返回结果值
  *
  */
object Demo11_stream_Reduce {

    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

       val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 66), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 33), Tuple2(100, 10))

        dstream.keyBy(0)
            .reduce((kv1, kv2)=>(kv1._1, kv1._2 + kv2._2))
            .print("reduce --- ")

        env.execute("Reduce")
    }
}
