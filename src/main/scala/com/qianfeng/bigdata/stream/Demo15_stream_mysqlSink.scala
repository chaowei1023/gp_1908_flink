package com.qianfeng.bigdata.stream

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import com.qianfeng.bigdata.common.YQ
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  *
  * 原始数据：
  * date province add possible
  * 2020-7-1 beijing 1 2
  * 2020-7-2 beijing 2 1
  * 2020-7-3 beijing 1 0
  * 2020-7-3 tianjin 2 1
  *
  * 需求：
  * 1、算出每天、省份的adds、possible
  * 2、将如上计算结果打入到mysql中
  * 3、如果重启服务希望对已有的key进行累加，不是重算-----未做
  */
object Demo15_stream_mysqlSink {
    def main(args: Array[String]): Unit = {

        //1.获取流的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.获取source
        val dstream: DataStream[String] = env.socketTextStream("master", 6666)

        val res: DataStream[YQ] = dstream.map(line => {
            val fields: Array[String] = line.split(" ")
            val date: String = fields(0).trim
            val province: String = fields(1).trim
            val add: Int = fields(2).trim.toInt
            val possible: Int = fields(3).trim.toInt
            (date + "_" + province, (add, possible))
        })
            .keyBy(0)
            .reduce((kv1, kv2) => (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2)))
            .map(y => {
                val date_province: Array[String] = y._1.split("_")
                new YQ(date_province(0), date_province(1), y._2._1, y._2._2)
            })

        res.print("yq--")

       //将res结果打入mysql中
        res.addSink(new MyMysqlSink)

        env.execute("mysqlSink")
    }
}

//自定义输出需要实现OutputFormat
class MyMysqlSink extends RichSinkFunction[YQ] {
    //连接mysql的对象
    var conn: Connection = _
    var ps: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {

    }

    //将结果数据打入到mysql中
    override def invoke(yq: YQ, context: SinkFunction.Context[_]): Unit = {
        ps = conn.prepareStatement("replace into yq(dt, province, adds, possibles) values(?, ?, ?, ?)")

        //赋值
        ps.setString(1, yq.date)
        ps.setString(2, yq.province)
        ps.setInt(3, yq.add)
        ps.setInt(4, yq.possible)

        //执行插入
        ps.execute()

    }

    override def close(): Unit = {
        if (ps != null) {
            ps.close()
        }
        if (conn != null) {
            conn.close()
        }
    }

}