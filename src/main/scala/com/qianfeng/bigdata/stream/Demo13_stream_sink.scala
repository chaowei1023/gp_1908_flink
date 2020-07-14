package com.qianfeng.bigdata.stream

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * 流式基础的sink
  */
object Demo13_stream_sink {
    def main(args: Array[String]): Unit = {
        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 66), Tuple2(100, 65), Tuple2(200, 66), Tuple2(200, 56), Tuple2(200, 666), Tuple2(100, 33), Tuple2(100, 10))

        val res: DataStream[(Int, Int)] = dstream.keyBy(0)
            .reduce((kv1, kv2) => (kv1._1, kv1._2 + kv2._2))

        res.print("print sink ---")
        res.writeAsText("F:\\_NZ_1907班\\date\\out\\05", WriteMode.OVERWRITE)
        res.writeAsText("hdfs://master:9000/writeAsText", WriteMode.OVERWRITE)
        res.writeAsCsv("hdfs://master:9000/writeAsCsv", WriteMode.OVERWRITE)

        val resString: DataStream[String] = res.map(kv => s"${kv._1}, ${kv._2}")
        resString.print("string")
        //socket写出去的数据任然只支持一个并行度
        resString.writeToSocket("master", 6666, new SimpleStringSchema())

        env.execute("sink as Text")
    }
}
