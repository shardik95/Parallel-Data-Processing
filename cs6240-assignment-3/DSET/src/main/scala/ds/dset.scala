package ds

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DSetMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nds.DSetMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DSet")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val textFile = sc.textFile(args(0))

    /**
      * Reading input line by line,
      * since it is of form x,y, separating by comma
      * creating a new Row of form (y,1)
      * since y has x has follower
      */
    val rdd = textFile.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    })

    /**
      * Converting rdd to dataset using the schema
      */
    val sparkSession =  SparkSession.builder().getOrCreate()
    val dataset =  sparkSession.createDataset(rdd)

    /**
      * using groupBy to group same users and adding counts
      * using agg
      *
      * groupBy performs aggregation before shuffling
      */
    val counts = dataset.groupBy("_1").agg(sum($"_2"))
    counts.rdd.saveAsTextFile(args(1))

    println(counts.explain(true))

  }
}