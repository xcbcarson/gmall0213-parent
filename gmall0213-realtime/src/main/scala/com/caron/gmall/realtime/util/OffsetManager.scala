package com.caron.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @author Caron
 * @create 2020-07-19-9:06
 * @Description ${description}
 * @Version $version
 */
import scala.collection.JavaConversions._
object OffsetManager {
  def getOffset(topic:String ,consumerGroupId:String ): Map[TopicPartition,Long] = {
    val jedis = RedisUtil.getJedisClient
    val offsetKey = topic +  ":" + consumerGroupId

    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    if(offsetMap != null && offsetMap.size() > 0){
      val offsetList: List[(String, String)] = offsetMap.toList
      val offsetMyForKafka: Map[TopicPartition, Long] = offsetList.map {
        case (partition, offset) => {
          val topicPartition = new TopicPartition(topic, partition.toInt)
          println("加载偏移量： 分区：" + partition + "==>" + offset)
          (topicPartition, offset.toLong)
        }
      }.toMap
      offsetMyForKafka
    }else {
      null
    }
  }

  def saveOffset(topic:String, consumerGroupId:String, offsetRanges:Array[OffsetRange]): Unit = {
    val offsetKey = topic + ":" + consumerGroupId
    val offsetMap: util.Map[String, String] = new util.HashMap[String,String]()
    if(offsetRanges != null && offsetRanges.size > 0){
      for (offsetRange <- offsetRanges){
        val partition = offsetRange.partition.toString
        val untilOffset = offsetRange.untilOffset.toString // 结束
        println("保存偏移量： 分区：" + partition + "==>" + untilOffset)
        offsetMap.put(partition,untilOffset)
      }
    }

    val jedis : Jedis = RedisUtil.getJedisClient
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }


}
