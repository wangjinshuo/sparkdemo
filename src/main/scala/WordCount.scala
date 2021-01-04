import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.master","local")
    conf.set("spark.app.name","spark demo")
    val sc = new SparkContext(conf)
    var textIn = sc.textFile("hdfs://127.0.0.1:9000/2021/test.txt")
    println(textIn.first())
    val textSplit = textIn.flatMap(line=>line.split(" "))
    val textSplitFlag = textSplit.map(word=>(word,1))
    val countWord = textSplitFlag.reduceByKey((x,y)=>x+y)
    countWord.saveAsTextFile("./result")

  }
}
