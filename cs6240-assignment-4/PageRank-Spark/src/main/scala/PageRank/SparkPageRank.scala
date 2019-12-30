package PageRank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import scala.collection.mutable.ListBuffer;

object PageRankMain {
  
  def main(args: Array[String]) {
    var k: Int = 0;
    val iterations: Int = 1;

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nPageRank.PageRankMain <k> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // value of k is from MakeFile.
    k = args(0).toInt

    var PRList= new ListBuffer[Row]()
    var EdgeList = new ListBuffer[Row]()

    PRList += Row(0, 0.0)
    val initialPageRank = 1.0/(k*k)

    /**
      * Creating initial Graph and PageRank as DataFrame (tables).
      */
    for(i <- 1 to (k*k)) {
      if(i%k == 0) {
        EdgeList += Row(i,0);
      } else {
        EdgeList += Row(i, i+1)
      }
      PRList += Row(i,initialPageRank)
    }

    val graphSchema = new StructType()
      .add(StructField("Edge1", IntegerType, true))
      .add(StructField("Edge2", IntegerType, true))

    val graph = sqlContext.createDataFrame(sc.parallelize(EdgeList), graphSchema)

    val pageRankSchema = new StructType()
      .add(StructField("Edge", IntegerType, true))
      .add(StructField("PageRank", DoubleType, true))

    val pageRank = sqlContext.createDataFrame(sc.parallelize(PRList), pageRankSchema)

    /**
      * +-----+-----+
      * |Edge1|Edge2|
      * +-----+-----+
      * |    1|    2|
      * |    2|    3|
      * |    3|    0|
      * |    4|    5|
      * |    5|    6|
      * |    6|    0|
      * |    7|    8|
      * |    8|    9|
      * |    9|    0|
      * +-----+-----+
      */
//    println(graph.show());

    /**
      * +----+------------------+
      * |Edge|          PageRank|
      * +----+------------------+
      * |   0|               0.0|
      * |   1|0.1111111111111111|
      * |   2|0.1111111111111111|
      * |   3|0.1111111111111111|
      * |   4|0.1111111111111111|
      * |   5|0.1111111111111111|
      * |   6|0.1111111111111111|
      * |   7|0.1111111111111111|
      * |   8|0.1111111111111111|
      * |   9|0.1111111111111111|
      * +----+------------------+
      */
//    println(pageRank.show());

    /**
      * Converting dataframe graph table into rdd's and Caching
      */
    val graphRDD = graph
                    .rdd
                    .map(row => (row(0), List(row(1))))
                    .reduceByKey(_ ++ _).cache()

    /**
      * Converting dataframe pageRank table into Rdd.
      * and using same partitioner as graphRDD.
      */
    var pageRankRDD = pageRank.rdd.map(row => (row(0), row(1).toString.toDouble))
                              .partitionBy(graphRDD.partitioner.get)

    // Iterating for 10 iterations.
    for(_ <- 1 to iterations) {

      /**
        * Calculating the outgoing contribution by joining graphRDD and PageRankRDD.
        *  Taking care of pages with no inlink with appending (page, 0.0)
        */
      val contribs = graphRDD.join(pageRankRDD).flatMap {
        case (page,(edges, rank)) => val size = edges.size
          edges.flatMap(edge => List((edge, rank/size), (page, 0.0)))
      }.reduceByKey(_+_)

      /**
        * Find the dangling page mass.
        */
      val delta = contribs.lookup(0).head

      /**
        * calculating the new pagerank using the formula and setting
        * the dangling page rank to 0.0
        */
      pageRankRDD = contribs.map{
        case(edge, value) => if (edge.toString.toInt == 0) {
          (edge, 0.0)
        } else (edge, 0.15*(1.0/(k*k)) + 0.85 * (value + delta*(1.0/(k*k))))
      }
    }

    logger.info(pageRankRDD.toDebugString)

    pageRankRDD.saveAsTextFile(args(1))

//    val output = pageRankRDD.collect()
//    val sum = output.filter(edge => edge._1!=0).map(edge=>edge._2).sum
//    println("PageRank sum: "+ sum)
  }
}