package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDAMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrdd.RDDAMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-A")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))

    val addToCount = (count1: Int, count2: Int) => count1 + count2
    val sumPartitionCount = (p1: Int, p2: Int) => p1 + p2

    /**
      * Reading input line by line,
      * since it is of form x,y, separating by comma
      * creating a new rdd of form (y,1)
      * since y has x has follower
      *
      *
      * using aggregateByKey to group all users and add the counts,
      * performing in-mapper combining using addToCount function,
      * adding partition using sumPartitionCount
      * with initial count as  0
      */
    val followerCount = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    }) .aggregateByKey(0)(addToCount, sumPartitionCount)

    followerCount.saveAsTextFile(args(1))
    println(followerCount.toDebugString)
  }
}