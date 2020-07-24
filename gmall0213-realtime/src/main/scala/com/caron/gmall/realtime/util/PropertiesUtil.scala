package com.caron.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Caron
 * @create 2020-07-17-22:25
 * @Description ${description}
 * @Version $version
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
