package com.qzw.test

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @author : qizhiwei
 * @date : 2021/3/1
 * @Description : ${Description}
 */
object IcebergTest {
  def main(args: Array[String]): Unit = {
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val tbEnv = TableEnvironment.create(blinkBatchSettings)
    tbEnv.executeSql("CREATE CATALOG hive_catalog WITH (" +
      "  'type'='iceberg'," +
      "  'catalog-type'='hive'," +
      "  'hive-conf-dir'='/Users/qizhiwei/Downloads/apache-hive-1.2.2-bin/conf/'" +
      ")")

    tbEnv.useCatalog("hive_catalog")
//    tbEnv.executeSql("create database iceberg_db")
    tbEnv.useDatabase("iceberg_db")
    tbEnv.executeSql("CREATE TABLE test_table (id int, data string) with ('write.format.default'='parquet','engine.hive.enabled'='true')")
    tbEnv.executeSql("show tables").print()
    tbEnv.executeSql("INSERT INTO test_table SELECT 1, 'a'")
    tbEnv.executeSql("select * from test_table").print()

  }
}
