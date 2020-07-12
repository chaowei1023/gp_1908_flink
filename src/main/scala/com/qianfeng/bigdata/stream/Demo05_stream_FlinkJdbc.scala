package com.qianfeng.bigdata.stream

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.types.Row

/**
  * 使用flinkjdbc来读取有jdbc驱动数据库中数据（维度数据，读取增量数据场景）
  *
  * 1.添加jdbc的依赖
  */
object Demo05_stream_FlinkJdbc {
    def main(args: Array[String]): Unit = {
        //获取执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2.自定义jdbc的source
        val fields_type: Array[TypeInformation[_]] = Array[TypeInformation[_]](
            //id字段类型
            BasicTypeInfo.INT_TYPE_INFO,
            //name字段类型
            BasicTypeInfo.STRING_TYPE_INFO
        )

        //获取行信息
        val rowTypeInfo = new RowTypeInfo(fields_type:_*)

        //获取flink-jdbc的InputFormat输入格式
        val jDBCInputFormat: JDBCInputFormat = JDBCInputFormat
            .buildJDBCInputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl("jdbc:mysql://master:3306/test")
            .setUsername("root")
            .setPassword("123456")
            .setQuery("select * from stu1")
            .setRowTypeInfo(rowTypeInfo)
            .finish()

        //获取jdbc-inputformat中的数据
        val res: DataStreamSource[Row] = env.createInput(jDBCInputFormat)

        //持久化
        res.print("flink-jdbc-")

        //触发执行
        env.execute("flink jdbc inputformat-")

    }
}
