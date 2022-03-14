package com.citi.stream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

//自定义数据源
object CustomSource_MultyThread {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //基于SourceFunction接口实现的数据源，这个只是单并行度度数据源
    val stream = env.addSource(new ParallelSourceFunction[String] {
      var flag = true

      //发射数据
      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while (flag) {
          sourceContext.collect("hello" + random.nextInt(1000))
          Thread.sleep(500)
        }
      }

      //停止
      override def cancel(): Unit = {
        flag = false
      }
    }).setParallelism(2)

    stream.print()

    env.execute()

  }

}
