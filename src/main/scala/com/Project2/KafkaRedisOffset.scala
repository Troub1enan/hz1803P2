package com.Project2

import java.lang

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(3))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "zk002"
    // topic
    val topic = "czyczy"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.62.102:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    // 对数据进行处理
    val file = ssc.sparkContext.textFile("E:\\千峰学习资料\\Spark\\项目（二）01\\充值平台实时统计分析\\city.txt")
    val map: Map[String, String] = file.map(t=>(t.split(" ")(0),t.split(" ")(1))).collect().toMap
    //注意，这里面的存储与方式是Map["100"->"北京","200"->广东"]
    // 将数据进行广播
    //注意，我们无法广播RDD
    val broad = ssc.sparkContext.broadcast(map)
    //循环这个批次中的RDD
    stream.foreachRDD({
      rdd=>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        //获取数据,循环rdd获取其中的一条数据
        val baseData: RDD[(String, String, String, List[Double], (String, String))] = rdd.map(_.value()).map(t=>JSON.parseObject(t))
          // 过滤需要的数据（充值通知）
          //filter和filterNot以返回真和假
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(t=>{
            // 先判断一下充值结果是否成功
            val result = t.getString("bussinessRst") // 充值结果
            val money:Double = if(result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
            val feecount = if(result.equals("0000"))  1 else 0 // 充值成功数
            val starttime = t.getString("requestId") // 开始充值时间
            val stoptime = t.getString("receiveNotifyTime") // 结束充值时间
            val pro = t.getString("provinceCode") // 省份编码
            val province: String = broad.value.get(pro).get // 根据省份编码取到省份名字
            // 充值时长
            val costtime = Utils_Time.consttiem(starttime,stoptime)
            (starttime.substring(0,8)//天
              ,starttime.substring(0,12)//分钟
              ,starttime.substring(0,10)//小时
              ,List[Double](1,money,feecount,costtime)
              ,(province,starttime.substring(0,10))//省份和登陆时间
            )
          }).cache()
        // 指标一 1
        //list1和list2分别表示的这个批次和下一个批次
        val result1: RDD[(String, List[Double])] = baseData.map(t=>(t._1,t._4)).reduceByKey((list1, list2)=>{
          // list1(1,2,3).zip(list2(1,2,3)) = list((1,1),(2,2),(3,3))
          // map处理内部的元素相加
          list1.zip(list2).map(t=>t._1+t._2)

        })
        JedisAPP.Result01(result1)
        // 指标一 2
        val result2 = baseData.map(t=>(t._3,t._4.head)).reduceByKey(_+_)
        JedisAPP.Result02(result2)
        // 指标 二
        val result3: RDD[((String, String), List[Double])] = baseData.map(t=>(t._5,t._4)).reduceByKey((list1, list2)=>{list1.zip(list2).map(t=>t._1+t._2)})
        JedisAPP.Result03(result3)

        //指标三,表示每个省份，订单量
        val result4 = baseData.map(t=>(t._5._1,1)).reduceByKey(_+_)
        JedisAPP.Result04(result4)
        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
