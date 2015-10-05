package com.aironman.samples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
 * Because the cassandra driver will return all cells for a given rowkey in the same partition,
 * aggregating using mapPartitions can be more efficient
 */
object PartitionGroupingExample {
  def main(args: Array[String]): Unit = {
    // only setting app name, all other properties will be specified at runtime for flexibility
    //to test in local mode, just decomment setMaster and the cassandra host ip
    val conf = new SparkConf().setAppName("cassandra-example-partition-grouping")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val visits = sc.cassandraTable[(String)]("test", "user_visits").
      select("user")

    val visitsPerUser = visits.map { user =>
      (user, 1)
    }.mapPartitions { userIterator =>
      new GroupByKeyIterator(userIterator)
    }.mapValues(_.size)

    val maxVisits = visitsPerUser.values.max

    sc.stop

    println(maxVisits)

    println("Done!")
  }
}
