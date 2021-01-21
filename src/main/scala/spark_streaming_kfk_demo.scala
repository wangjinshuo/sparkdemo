import com.alibaba.fastjson.{JSON, JSONException}
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class LogBean(id:String,
                   data1:String,
                   data2:String,
                   data3:String,
                   data4:String,
                   data5:String,
                   data6:String,
                   data7:String,
                   data8:String,
                   data9:String,
                   data10:String,
                   data11:String,
                   data12:String,
                   data13:String,
                   data14:String,
                   data15:String,
                   data16:String,
                   data17:String,
                   data18:String,
                   data19:String,
                   data20:String)

object spark_streaming_kfk_demo {
  private val logger = LoggerFactory.getLogger("warning")
  def main(args: Array[String]): Unit = {
    val checkpointPath = "/data/output/checkpoint/kafka-direct"
    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(checkpointPath)

    //kafka
    val config = Map[String,String](
      "bootstrap.servers"->"192.168.1.101:9092",
      "auto.offset.reset" -> "latest",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "group.id"->"group01"
    )
    val topics = Set("test0")
    //创建Dstream
    val kafkaDstream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,config))

    kafkaDstream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val value = record.value()
        println(value)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
