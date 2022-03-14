package com.citi.stream.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object MapOperator {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //使用flatmap 代替filter
    val stream = env.socketTextStream("risus1", 8888)

    stream.flatMap(x => {
      val rest = new ListBuffer[String]

      if (!rest.contains("abc")){
        rest += x
      }
      rest.iterator
    }).print()


    //keyBy算子: 分流算子  根据用户指定度字段来分组
    stream.flatMap(_.split(" "))
      .map((_, 1))
//      .keyBy(new KeySelector[(String, Int), String] {
//        override def getKey(in: (String, Int)): String = {
//
//          in._1
//        }
//      })
      .keyBy(x => x._1)
//      .sum(1)
      .reduce((v1,v2) => (v1._1, v1._2+v2._2))
      .print()

    env.execute()


  }

}
