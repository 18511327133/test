package cn._51doit.sorce


import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
object WordCount {


  def main(args: Array[String]): Unit = {

    // parse input arguments
    /*val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic>"
        + "--output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--group.id <some id> [--prefix <prefix>]")
      return
    }*/

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //禁止log
    env.getConfig.disableSysoutLogging
    //设置重启策略
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    env.enableCheckpointing(5000)
    //设置参数。make parameters available in the web interface
    //  env.getConfig.setGlobalJobParameters(params)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "zjj1:9092")
    // only required for Kafka 0.8
  //  properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test1")


    // create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer = new FlinkKafkaConsumer010(
    "test001",
      new SimpleStringSchema,
      properties )
    kafkaConsumer.setStartFromEarliest()

    val inputStream: DataStream[String] = env.addSource(kafkaConsumer)

    //val filtered: DataStream[String] = inputStream.filter(!_.startsWith("hello"))

    //触发Sink，设置输出（相当于Spark的Foreach）



   /* val jsonDS = env.addSource(kafkaConsumer)
      .map(line => {
        //函数是在Driver定义的，但是是在Executor中执行的
        var jsonLine: String = null
        try {
          JSON.parseObject(line, classOf[LogBean])
          jsonLine = line
        } catch {
          case e: JSONException => {
            //logger.error("parse json error, error line is : " + lines)
          }
        }
        jsonLine
      })
      .filter(_ != null)


    jsonDS.addSink(line => {
      println(line)
    })*/

   // 将数据下写回到Kafka create a Kafka producer for Kafka 0.10.x
        val kafkaProducer = new FlinkKafkaProducer010(
         "test003",
          new SimpleStringSchema,
          properties)
    val value = inputStream.filter(_.startsWith("h"))
    value.print()
    //添加一个Sink write data into Kafka
    value.addSink(kafkaProducer)



    //启动任务
    env.execute("Kafka 0.10 Example")
  }

}
