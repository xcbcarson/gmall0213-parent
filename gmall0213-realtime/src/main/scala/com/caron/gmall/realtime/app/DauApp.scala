package com.caron.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.caron.gmall.bean.DauInfo
import com.caron.gmall.realtime.util.{MyESUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author Caron
 * @create 2020-07-17-22:16
 * @Description ${description}
 * @Version $version
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val groupId = "dau_group"
    val topic = "GMALL_START0213"
    //从Redis中读取偏移量
    val offsetMyForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    //将偏移量传递给kafka
    if(offsetMyForKafka != null && offsetMyForKafka.size > 0){
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMyForKafka,groupId)
    }else{
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //从流中获取本批次的偏移量结束点
    var ranges: Array[OffsetRange] = null //当前偏移量的变化状态
    val recordInputeDS: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => { //周期性在Driver中执行
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val startLogInfoDStream: DStream[JSONObject] = recordInputeDS.map(
      record => {
        val jsonString = record.value()
        // 把json变成对象map jsonObject  case class
        val startupJSONObj: JSONObject = JSON.parseObject(jsonString)
        startupJSONObj
    })
    //优化后
    val jsonObjFilterDStream: DStream[JSONObject] = startLogInfoDStream.mapPartitions(
      jsonObjItr => {
        val beforeList = jsonObjItr.toList
        println("过滤前：" + beforeList.size)
        val jsonObList = new ListBuffer[JSONObject]
        val jedis: Jedis = RedisUtil.getJedisClient
        for (jsonObj <- beforeList) {
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val ts = jsonObj.getLong("ts")
          val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
          val dauKey = "dau:" + dt
          val flag = jedis.sadd(dauKey, mid)
          if (flag == 1L) {
            jsonObList.append(jsonObj)
          }
        }
        jedis.close()
        println("过滤后：" + jsonObList.size)
        jsonObList.toIterator
      }
    )
    //jsonObjFilterDStream.print(1000)

    jsonObjFilterDStream.foreachRDD(
      rdd => {
        //rdd.foreach(jsonObj => println(jsonObj))  //写入数据库的操作

        //写入到es的操作
        rdd.foreachPartition(
          jsonObjItr => {
            val formattoday = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val jsonObjlist: List[JSONObject] = jsonObjItr.toList

            val dauWithIdList: List[(DauInfo, String)] = jsonObjlist.map(
              jSONObj => {

                val commonJSONObj = jSONObj.getJSONObject("common")
                val startJson = jSONObj.getJSONObject("start")
                val ts: Long = jSONObj.getLong("ts")
                val dateTimeString: String = formattoday.format(new Date(ts))
                //获取日期 、小时、分钟
                val dateArr = dateTimeString.split(" ")
                val dt = dateArr(0)
                val time = dateArr(1)
                val timeArr = time.split(":")
                val hr = timeArr(0)
                val mi = timeArr(1)

                val dauInfo: DauInfo = DauInfo(
                  commonJSONObj.getString("mid"),
                  commonJSONObj.getString("uid"),
                  commonJSONObj.getString("ar"),
                  commonJSONObj.getString("ch"),
                  commonJSONObj.getString("vc"),
                  dt,
                  hr,
                  mi,
                  ts
                )
                (dauInfo, dauInfo.mid) //日活表（按天切分索引)
              }
            )

            val today = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkSave(dauWithIdList,"gmall_dau_info0213_" + today)
          }
        )


        OffsetManager.saveOffset(topic,groupId,ranges) //要在driver中执行 周期性 每批执行一次
      }
    )

    //优化前
    /*startLogInfoDStream.filter{
      jsonOb => {
        val mid = jsonOb.getJSONObject("common").getString("mid")
        val ts = jsonOb.getLong("ts")
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

        val jedis: Jedis = RedisUtil.getJedisClient
        val dauKey = "dau:" + dt
        val flag = jedis.sadd(dauKey,mid)

        if(flag == 1L){
          true
        }else{
          false
        }
        jedis.close()
      }
        null
    }*/

    // 1  去重  把启动日志 只保留首次启动 （日活）
    //  redis去重
    // 去重  type:set  sadd     type:String setnx  既做了判断 又做了写入
    //set 当日的签到记录保存在一个set中   便于管理  整体查询速度快
    //string  把当天的签到记录 散列存储在redis   当量级巨大时 可以利用集群 做分布式

    //      // 取出来 mid
    //      //    用mid 保存一个清单 （set）
    //      // redis  type ? set   key?  dau:2020-07-17   value ? mid
    //      //  用sadd 执行
    //      // 判断返回值 1或0  1 保留数据 0 过滤掉
    //      null
    //    }
    //得到一个过滤后的Dstream

    //写到es 中

    ssc.start()
    ssc.awaitTermination()
  }
}
