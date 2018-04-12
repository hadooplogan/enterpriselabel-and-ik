package Util

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import DataConvertUtil.exists
import DataConvertUtil.mkdir
import org.apache.derby.impl.sql.compile.TableName
import org.apache.hadoop.fs.FileSystem

object DataFrameUtils {
  private val UNCACHE_TABLE: Int = -1;
  private val CACHETABLE_LAZY: Int = 0;
  private val CACHETABLE_EAGER: Int = 1;
  private val CACHETABLE_MAGIC: Int = 2;

  def getDataFrame(sparkSession: SparkSession, hql: String, tableName: String, CacheMode: Int): DataFrame = {

    val df = sparkSession.sqlContext.sql(hql).createOrReplaceTempView(tableName)

    CacheMode match {

      case -1 => val df: DataFrame = sparkSession.sqlContext.sql(hql)
        df.createOrReplaceTempView(tableName)
        df

      case 0 => val df: DataFrame = sparkSession.sqlContext.sql(hql)
        df.cache()
        df
      case 1 => sparkSession.sqlContext.sql(hql).createOrReplaceTempView(tableName)
        sparkSession.sqlContext.sql("CACHE TABLE" + tableName)


      case 2 =>
        val df: DataFrame = sparkSession.sqlContext.sql("CACHE TABLE" + tableName + "AS" + hql)
        df.createOrReplaceTempView(tableName)
        df

      case _ => val df: DataFrame = sparkSession.sqlContext.sql(hql)
        df.createOrReplaceTempView(tableName)
        df
    }


  }


  def getDataFrame(sparkSession: SparkSession, hql: String, tableName: String): DataFrame = {
    val df = sparkSession.sqlContext.sql(hql)

    df.createOrReplaceTempView(tableName)

    df
  }


  def saveAsParquetOverWrite(df: DataFrame, path: String, hdfs: FileSystem) {
    DataConvertUtil.deletePath(hdfs, path)
    if (DataConvertUtil.exists(hdfs, path)) DataConvertUtil.mkdir(hdfs, path)

    df.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
