package com.qianfeng.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * java版本的批次词频统计
 */
public class Demo02_Batch_WC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.获取source
        DataSource<String> dsource = env.fromElements("flink is good flink is nice flink flink flink");

        //3.基于dataSource来做transformation
        AggregateOperator<Tuple2<String, Integer>> sumed = dsource.flatMap(new SplitFunction())
                .groupBy(0)
                .sum(1);

         //4.结果sink
        sumed.print();
        //5.触发执行
        //env.execute("java_batch_wc");

    }


    //单独封装split
    public static class SplitFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] fields = s.split(" ");

            for (String field : fields) {
                //循环输出
                collector.collect(new Tuple2<String, Integer>(field, 1));
            }
        }
    }
}
