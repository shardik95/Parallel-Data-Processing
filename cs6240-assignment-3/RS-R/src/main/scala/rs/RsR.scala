package rs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.LogManager

object RsRMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrs.RsR <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Rs R")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    /**
      * Reading input line by line,
      * since it is of the form x,y, separated by comma
      * creating a new pair rdd of form (y,1)
      * since y has x has follower
      */
    val XtoY =
      textFile.map(line => {
        line.split(",")
      }).filter(users => users(0).toInt < 40000 && users(1).toInt < 40000)
        .map(users => (users(0), users(1)))

    /**
      * creating RDD of form (y,z) by inverting XtoY since
      * we want to join on y to get path of length 2.
      */
    val YtoZ = XtoY.map {
      case (user1, user2) => (user2, user1)
    }

    /**
      * Join XtoY and YtoZ on key Y, such that X!=Z
      * and transform the result to ((Z,X),Y) for the next join
      */
    val pathLength2 = XtoY.join(YtoZ).filter(joinedRDD => {
      val WV = joinedRDD._2
      WV._1 != WV._2
    }).map {
      case (userY, (userZ, userX)) => ((userZ, userX), userY)
    }

    /**
      * create ZtoX with key as (Z,X) since we want to join on (Z,X)
      */
    val ZtoX = XtoY.map {
      case (user1, user2) => ((user1, user2), "")
    }

    /**
      * Join X->Y->Z with Z->X
      */
    val socialTriangle = pathLength2.join(ZtoX)

    println("SocialTriangleCount: "+ socialTriangle.count()/3)

  }
}