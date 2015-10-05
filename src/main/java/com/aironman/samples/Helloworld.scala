package com.aironman.samples

import org.apache.spark._
import com.datastax.spark.connector._

object Helloworld {
  def main(args: Array[String]): Unit = {
    // only setting app name, all other properties will be specified at runtime for flexibility
    //to test in local mode, just decomment setMaster and the cassandra host ip
    val conf = new SparkConf().setAppName("Helloworld")
                              .setMaster("local[4]")
                              .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val hello = sc.cassandraTable[(String, String)]("test", "hello")

    val first = hello.first

    sc.stop

    println(first)
    
    println("Done!")
  }

}