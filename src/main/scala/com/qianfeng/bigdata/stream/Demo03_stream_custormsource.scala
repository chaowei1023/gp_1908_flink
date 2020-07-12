package com.qianfeng.bigdata.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  流式的自定义的source
  1.继承SourceFunction:
  2.RichSourceFunction：比继承SourceFunction多继承AbstractRichFunction，所以多open()和close()
  3.ParallelSourceFunction：和SourceFunction相比，可以设置并行度
  4.RichParallelSourceFunction：和RichSourceFunction相比，可以设置并行度
  *
  */
object Demo03_stream_customsource {

    def main(args: Array[String]): Unit = {

        //获取执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //添加源
        val dstream: DataStream[String] = env.addSource(new SourceFunction[String] {
            //生产数据--打到下游
            override def run(ctx: SourceFunction.SourceContext[String]) = {
                val random = new Random()
                //循环产生数据
                while (true) {
                    val age: Int = random.nextInt(100)

                    ctx.collect("随机年龄:" + age)

                    Thread.sleep(age)
                }
            }

            //取消生产数据，控制run()结束运行
            override def cancel() = ???

        })

        dstream.print("my sourceFunction-")


        env.execute("Demo03_stream_customsource")
    }

}
