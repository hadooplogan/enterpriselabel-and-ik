package main

import org.apache.spark.sql.SparkSession
import Util.{DataFrameUtils, logs}
import common.CommonConfig
import org.apache.hadoop.fs.FileSystem
import sql.BaseInfoKpi
import common.Constant

object BaseKpiETL extends logs {
  def main(args: Array[String]) {
    //sparksession程序入口
    val spark = SparkSession.
      builder().
      appName("Chinadaas-deal-enterprise-label").
      enableHiveSupport().
      getOrCreate()
    val hdfs = FileSystem
    execBase(spark, hdfs)

    spark.stop()
  }


  def execBase(spark: SparkSession, hdfs: FileSystem) = Unit {
    info("start to anylaze base enterprise lable")
    DataFrameUtils.saveAsParquetOverWrite(BaseInfoKpi.BaseIfo(spark), CommonConfig.getValue(Constant.ENT_INDEX_BASE_DIR), hdfs)


  }
}
