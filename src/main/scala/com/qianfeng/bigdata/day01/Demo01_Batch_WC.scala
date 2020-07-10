package com.qianfeng.bigdata.day01

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}

//隐式转换
import org.apache.flink.api.scala._
/**
  * 统计词频  scala版本
  */
object Demo01_Batch_WC {

    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println(
                """
                  |<outPath>
                  | null
                """.stripMargin)
        }

        //1.初始化执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //2.使用env操作即可 --source（需要假如scala的隐式）
        val data: DataSet[String] = env.fromElements("flink is good flink is nice flink flink flink")

        //3.针对source来做转换 --- transformation
        val sumed: AggregateDataSet[(String, Int)] = data.flatMap(line => line.split(" "))
            .map((_, 1))
            .groupBy(0)
            .sum(1)

        //4.针对转换后的值进行存储 -- sink
        sumed.setParallelism(1)
        sumed.writeAsText(args(0))

        //5.启动程序执行
        env.execute("batch-scala-wc")
    }
}
