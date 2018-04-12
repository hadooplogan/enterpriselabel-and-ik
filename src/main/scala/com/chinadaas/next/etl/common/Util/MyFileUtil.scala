package Util

import scala.collection.mutable
import scala.io.Source

/**
  * 文件处理工具包
  */

object MyFileUtil {

  /** 读取文件
    * 组装配置参数hashmap
    * 全量
    */


  def getCfg(path: String): mutable.HashMap[String, String] = {
    var cfg = new mutable.HashMap[String, String]()
    val file = Source.fromFile(path)
    val br = file.bufferedReader()
    var s: String = br.toString
    while (s != null) {
      cfg.put(s.substring(0, s.indexOf(",")), s.substring(s.lastIndexOf(",") + 1).replace("\\", ""))
    }
    br.close()
    cfg
  }

  /**
    * 非全量参数 增量数据参数 hashmap
    *
    */


}
