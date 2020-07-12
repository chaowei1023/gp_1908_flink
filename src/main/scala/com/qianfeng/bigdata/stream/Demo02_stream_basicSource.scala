package com.qianfeng.bigdata.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object Demo02_stream_basicSource {

    def main(args: Array[String]): Unit = {

        //获取执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //1.基于集合
        env.fromElements("i like flink", "flink").print("fromElements-")


        //创建一个集合
        val list: ListBuffer[Int] = new ListBuffer[Int]()
        list += 10
        list += 20
        list += 30
        list += 40

        env.fromCollection(list).filter(x => x > 20).print("fromCollection-")


        //2.基于文件
        env.readTextFile("F:\\_NZ_1907班\\date\\words.txt", "UTF-8").print("readTextFile")
        env.readTextFile("hdfs://master:9000/data/test01.txt", "UTF-8").print("readTextFile")

        //3.基于socket --不支持设置并行度，默认只能是1个
        env.socketTextStream("master", 6666)
            //.setParallelism(3)
            .print("socketTextStream")

        //触发执行
        env.execute("basicSource")

    }

}
