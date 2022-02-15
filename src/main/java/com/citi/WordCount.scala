package com.citi

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    /**
     * createLocalEnvironment 创建一个本地执行环境 local
     * createLocalEnvironmentWithWebUI 创建一个本地执行环境，同时还开启web UI的查看端口，默认端口是8081
     * getExecutionEnvironment 根据你执行的环境创建上下文，比如local cluster
     * 建议使用 getExecutionEnvironment
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * DataStream 一组相同类型的元素组成的数据流
     */
    val initStream:DataStream[String] = env.socketTextStream("risus1", 8888)

    val wordStream = initStream.flatMap(_.split(" "))
    val pairStream = wordStream.map((_, 1))
    val keyByStream = pairStream.keyBy(0)
    val restStream = keyByStream.sum(1)

    restStream.print()


    //启动flink任务
    env.execute("first flink job")

    /**
     * nc -lk 8888
     *
     * 4> (msb,,1)
      *5> (hello,1)
      *5> (hello,2)
      *4> (msb,,2)
      *2> (,,1)
      *12> (msb,1)
      *5> (hello,3)
     * 相同的数据一定是由同一个线程处理
     */


  }

}
