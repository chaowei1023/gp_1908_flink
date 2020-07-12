package com.qianfeng.bigdata.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.qianfeng.bigdata.common.Stu
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * 读取mysql的数据
  */
object Demo04_stream_mysqlsource {
    def main(args: Array[String]): Unit = {
        //获取执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //添加源
        val dstream: DataStream[Stu] = env.addSource(new MySqlSourceParallel())

        dstream.setParallelism(3).print("my sourceFunction-")


        env.execute("Demo03_stream_customsource")
    }
}


/*
CREATE TABLE `stu1` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

1、泛型为输出的数据类型
 */
class MyMysqlSource() extends RichSourceFunction[Stu] {
    //连接数据库的对象
    var conn: Connection = _
    var ps: PreparedStatement = _
    var rs: ResultSet = _


    override def open(parameters: Configuration): Unit = {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://master:3306/test"
        val user = "root"
        val password = "123456"
        //反射构建Driver
        Class.forName(driver)
        try {
            conn = DriverManager.getConnection(url, user, password)
            //查询
            ps = conn.prepareStatement("select * from stu1")
        } catch {
            case e: SQLException => e.printStackTrace()
        }
    }

    //往下游打数据
    override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
        try {
            rs = ps.executeQuery()
            while (rs.next()) {
                val id = rs.getInt(1)
                val name = rs.getString(2)
                val stu = new Stu(id, name)
                //输出
                ctx.collect(stu)
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        }

    }


    override def cancel(): Unit = {}

    override def close(): Unit = {
        if (rs != null) {
            rs.close()
        }
        if (ps != null) {
            ps.close()
        }
        if (conn != null) {
            conn.close()
        }

    }
}

class MySqlSourceParallel extends RichParallelSourceFunction[Stu] {
    var conn: Connection = _
    var ps: PreparedStatement = _
    var rs: ResultSet = _

    override def open(parameters: Configuration): Unit = {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://master:3306/test"
        val user = "root"
        val password = "123456"
         Class.forName(driver)
        try {
            conn = DriverManager.getConnection(url, user, password)
            ps = conn.prepareStatement("select * from stu1")
        } catch {
            case e: SQLException => e.printStackTrace()
        }
    }

    override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
        try {
            rs = ps.executeQuery()
            while (rs.next()) {
                val id = rs.getInt(1)
                val name = rs.getString(2)
                val stu = new Stu(id, name)
                ctx.collect(stu)
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        }
    }

    override def cancel(): Unit = {

    }

    override def close(): Unit = {
        if (rs != null) {
            rs.close()
        }
        if (ps != null) {
            ps.close()
        }
        if (conn != null) {
            conn.close()
        }
    }
}