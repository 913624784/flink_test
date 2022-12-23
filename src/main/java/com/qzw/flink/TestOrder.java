package com.qzw.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: qizhiwei
 * @date: 2022/2/23
 * @PackageName: com.qzw.flink
 * @Description:
 */
public class TestOrder {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60 * 5);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60 * 10);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        Row row1 = Row.of(1, "hello", 10, "2022-03-03 12:00:00");
        Row row2 = Row.of(1, "world", null, "2022-03-03 13:00:00");
        DataStreamSource<Row> source = env.fromElements(row1, row2);
        Table table = tableEnv.fromDataStream(source);
        tableEnv.createTemporaryView("table", table);

        //注意LAST_VALUE返回的是最新的非NULL数据，如果该字段可能为NULL，直接使用LAST_VALUE可能导致错误的结果，例如
        Table table0 = tableEnv.sqlQuery("select f0, last_value(f1), last_value(f2) from `table` group by f0");
        //保序 使用LAG(col, 0)获取当前行，如果当前行为null，直接用当前行，当前行不是null，取last_value
        Table table1 = tableEnv.sqlQuery("select f0, last_value(f1), if(LAG(f2, 0) is null, LAG(f2, 0), last_value(f2)) from `table` group by f0");
//        Table table2 = tableEnv.sqlQuery("select f0, last_value(f1), LAG(f2, 0) from `table` group by f0");
        Table table3 = tableEnv.sqlQuery("select *\n" +
                "  from (\n" +
                "        select *,\n" +
                "               row_number() over(partition by f0 order by f3 desc) as rn\n" +
                "          from `table`\n" +
                "       )\n" +
                " where rn = 1");
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table3, Row.class);
        dataStream.filter(booleanRowTuple2 -> booleanRowTuple2.f0).print();

        env.execute();
    }
}
