package rep

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.broadcast

object RepDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nrep.RepDMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val textFile = sc.textFile(args(0))
    val XtoY =
      textFile.map(line => {
        line.split(",")
      }).filter(users => users(0).toInt < 100000 && users(1).toInt < 100000)
        .map(users => Row(users(0), users(1)))

    /**
      * Creating schema of type (id, val)
      * id: String
      * val: Integer
      * val refers to the followerCount
      */
    val schema = new StructType()
      .add(StructField("userIdX", StringType, true))
      .add(StructField("userIdY", StringType, true))

    /**
      * Converting rdd to dataframe using the schema
      */
    val df = sqlContext.createDataFrame(XtoY, schema)

    /**
      * Join XtoY and YtoZ to get length of path 2 on Key Y
      * such that X != Z.
      * Using broadcast to tell spark to use broadcast join.
      */
    val pathLength2 = df.select('userIdX as "df1_X", 'userIdY as "df1_Y").as("XtoY")
      .join(broadcast(df.select('userIdX as "df2_X", 'userIdY as "df2_Y").as("YtoZ")),
        $"XtoY.df1_Y" === $"YtoZ.df2_X" && $"XtoY.df1_X" =!= $"YtoZ.df2_Y")

    /**
      * Join Path Length 2 and XtoZ on key (Z,X)
      * Using broadcast to tell spark to use broadcast join.
      */
    val socialTriangle = pathLength2.as("Path")
                        .join(broadcast(df.as("ZtoX")),
                              $"Path.df2_Y" === $"ZtoX.userIdX" && $"Path.df1_X" === $"ZtoX.userIdY")

    println("Social Triangle Count" + socialTriangle.count()/3)
    println(socialTriangle.queryExecution.executedPlan)
  }
}