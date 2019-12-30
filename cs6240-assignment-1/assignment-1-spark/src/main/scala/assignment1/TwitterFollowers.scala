package assignment1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object TwitterFollowersMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nassignment1.TwitterFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Followers")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(args(0))
    val counts = textFile
                 .map(line => {
                        val user = line.split(",")(1)
                        (user,1)
                 })
                 .reduceByKey(_ + _)
    logger.info(counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}