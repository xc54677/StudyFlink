package com.citi.stream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ReadCollections {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val values = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8))

    values.print()

    env.execute();

  }

}
