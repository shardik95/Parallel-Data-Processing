package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDFMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrdd.RDDFMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-F")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))

    /**
      * Reading input line by line,
      * since it is of form x,y, separating by comma
      * creating a new rdd of form (y,1)
      * since y has x has follower
      *
      *
      * using FoldByKey to group all users and add the counts,
      * performing in-mapper combining, with initial count as 0
      */
    val followerCount = textFile.map(line => {
            val users = line.split(",")
            val followedUser = users(1)
            (followedUser, 1)
          }).foldByKey(0)((x,y) => x+y)

    println(followerCount.toDebugString)
    followerCount.saveAsTextFile(args(1))
  }
}