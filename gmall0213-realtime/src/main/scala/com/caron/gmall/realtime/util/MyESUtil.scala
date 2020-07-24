package com.caron.gmall.realtime.util

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}

import scala.collection.mutable.ListBuffer

/**
 * @author Caron
 * @create 2020-07-23-19:17
 * @Description ${description}
 * @Version $version
 */
object MyESUtil {
  private var factory: JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null) {
      build()
      factory.getObject
    }else{
      factory.getObject
    }
  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new
        HttpClientConfig.Builder("http://hadoop101:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .build())
  }

  def putIndex(): Unit = {

    val jest: JestClient = getClient
    val movieTest = Movie("102","中途岛之战")
    val datestring: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val index: Index = new Index.Builder(movieTest).index("gmall_dau_info0213_"+datestring)
      .`type`("_doc").id(movieTest.id).build()
    jest.execute(index)
    jest.close()

  }

  def queryFromEs()={
    val jest = getClient
    val query = "{ \n  \"aggs\": {\n    \"groupby_actor\": {\n      \"terms\": {\n        \"field\": \"actorList.name.keyword\"  \n      }\n    }\n  }\n}"
    val search :Search = new Search.Builder(query)
      .addIndex("movie_index").addType("movie").build()
    val result: SearchResult = jest.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[util.Map[String,Object]])
    val finalList = new ListBuffer[util.Map[String,Object]]
    import collection.JavaConversions._
    for (hit <- list){
      val source : util.Map[String,Object] = hit.source
      finalList.append(source)
    }
    println(finalList.mkString("\n"))

    jest.close()
  }
  //批次化操作
  def bulkSave(list:List[(Any,String)],indexName :String) = {
    val jest :JestClient = getClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    for ((doc,id) <- list){
      val index = new Index.Builder(doc).id(id).build() //如果给id则幂等性
      bulkBuilder.addAction(index)
    }
    val bulk = bulkBuilder.build()
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
    println("以保存" + items.size())
    jest.close()
  }

  def main(args: Array[String]): Unit = {
    putIndex()
  }

  case class Movie(id: String, movie_name: String)
}
