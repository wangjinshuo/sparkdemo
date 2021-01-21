import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import java.util.{Collections, Properties}
import java.io.{File, FileWriter, PrintWriter}
import java.time.Duration
import java.time.LocalDate
object kafka_demo {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "192.168.1.110:9092")
    prop.put("group.id", "group01")
    prop.put("auto.offset.reset", "latest")
    // 指定消费的key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("session.timeout.ms", "30000")
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    kafkaConsumer.subscribe(Collections.singletonList("test0"))
    while (true)
    {
      val records = kafkaConsumer.poll(Duration.ofMillis(1000))
      val nowdate = LocalDate.now().toString()
      val filename = "./data/"+nowdate+"-test0.dat"
      val file= new FileWriter(filename,true)
      val it = records.iterator()
      while (it.hasNext)
      {
        val next: ConsumerRecord[String, String] = it.next()
        file.write(next.value()+"\n")
        println("value=="+next.value())
      }
      file.close()
      System.gc()
    }
    kafkaConsumer.close()
  }
}
