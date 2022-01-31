import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object BKMeans_rdd{

  val conf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("ksahatova-bkmeans-rdd")

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val schema: StructType = StructType(Array(
    StructField("medallion", StringType, nullable = true),
    StructField("hack_license", StringType, nullable = true),
    StructField("vendor_id", StringType, nullable = true),
    StructField("rate_code", IntegerType, nullable = true),
    StructField("store_and_fwd_flag", StringType, nullable = true),
    StructField("pickup_datetime", TimestampType, nullable = true),
    StructField("dropoff_datetime", TimestampType, nullable = true),
    StructField("passenger_count", IntegerType, nullable = true),
    StructField("trip_time_in_secs", IntegerType, nullable = true),
    StructField("trip_distance", DoubleType, nullable = true),
    StructField("pickup_longitude", DoubleType, nullable = true),
    StructField("pickup_latitude", DoubleType, nullable = true),
    StructField("dropoff_longitude", DoubleType, nullable = true),
    StructField("dropoff_latitude", DoubleType, nullable = true)))

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val dataset = "small"
    val path = s"C:/Users/User/OneDrive - ITMO UNIVERSITY/VUB/BD/datasets/taxiTrips-${dataset}.csv"

    val data = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .load(path)

    val K = 20
    val df = data.select(col("pickup_longitude").as("longitude"),
                         col("pickup_latitude").as("latitude"))
                  .withColumn("cluster", lit(1))
                  .filter(col("longitude") =!= 0 && col("latitude") =!= 0).persist()


    println("*******Assign centroids using k-means for 2 clusters******")
    var m = k_means(df, 2)
    var Kc = 2
    var counter = 0
    println("*****Start while loop*****")
    while (Kc < K) {
      counter += 1
      println(s"*****${counter} iteration *****")
      val X = assign(df, m)
      //calculate the largest distance to the centroid
      val maxDistance = distance_bkm(X, m)
      //take the cluster of the farthest centroid
      val k = maxDistance.orderBy(desc("distance")).head().getAs[Int]("cluster")
      //obtain all points of this cluster
      val filteredClass = X.filter(col("cluster") === k)
      //split them again using naive k-means
      val mForFilteredClass = k_means(filteredClass, 2)
      //replace centroids of the filtered cluster with newly calculated by k_means
      m = replace(m, k, mForFilteredClass)
      Kc = Kc + 1
    }
    //df.unpersist()

    println("Centroids: ")
    println(m.mkString("\n"))
    println(s"****${dataset.toUpperCase} DATASET****")
    println(s"Execution time:  ${(System.currentTimeMillis() - startTime) / 1000}; " +
      s"number of iterations ${counter}; for k=${Kc} ")

  }

  def distance_bkm(df: DataFrame, m: Array[(Double, Double, Int)]): DataFrame = {

    val distances2centroids = udf((longitude: Double, latitude: Double, newClass: Int) => {
      val mi = m.filter(centroid => centroid._3 == newClass).head
      distance(longitude, latitude, mi._1, mi._2)})
    val dfWithDistances = df.withColumn("distances",  distances2centroids(df("longitude"), df("latitude"), df("cluster")))
    dfWithDistances.groupBy("cluster").agg(sum("distances").as("distance"))
  }

  def replace(m: Array[(Double, Double, Int)], k: Long, mForDividedCluster: Array[(Double, Double, Int)]):
  Array[(Double, Double, Int)] ={
    val mFiltered: Array[(Double, Double, Int)] = m.filter(m1 => m1._3 != k) ++ mForDividedCluster
    var i = 0
    mFiltered.map(m1 => {
      i += 1
      m1.copy(_3 = i)
    })
  }

  def k_means(df: DataFrame, K: Int): Array[(Double, Double, Int)] = {
    var continueLoop = true
    val esp = 1.0E-9

    var m = sample(df, K)
    var X = assign(df, m)

    while (continueLoop) {
      val mUpdated = updateCentroids(X)
      X = assign(X, mUpdated)
      var sumLong = 0D
      var sumLat = 0D
      var i = 0
      m.foreach(row => {
        sumLong += Math.pow(row._1 - mUpdated(i)._1, 2)
        sumLat += Math.pow(row._2 - mUpdated(i)._2, 2)
        i = i + 1
      })
      m = mUpdated
      if ((sumLong <= esp * K) && (sumLat <= esp * K)){
        continueLoop = false
      }
    }
    m
  }

  def sample(df: DataFrame, K: Int): Array[(Double, Double, Int)] = {
    var i = 0
    val centroids = df.limit(K) //.orderBy(rand())
    centroids.collect().map(x => {
      i = i + 1
      (x.getAs[Double]("longitude"),  x.getAs[Double]("latitude"), i)
    })
  }

  def distance(long1: Double, lat1: Double, long2: Double, lat2: Double): Double = {
    Math.pow(long2 - long1, 2) + Math.pow(lat2 - lat1, 2)
  }

  def assign(X: DataFrame, m: Array[(Double, Double, Int)]): DataFrame = {
    // recalculate the assigned class for each point

    val calcNewClass = udf((longitude: Double, latitude: Double) => {
      val array = m.map(centroid => {
        val distance2Class = distance(longitude, latitude, centroid._1, centroid._2)
        (centroid._1, centroid._2, centroid._3, distance2Class)
      })
      val newCentroid = array.sortBy(_._4) // sort or orderBy
      newCentroid(0)._3 // return class of the min distance
    })
    X.withColumn("cluster", calcNewClass(X("longitude"), X("latitude")))
  }

  case class Point(longitude: Double, latitude: Double)

  def parsePoint(row: Row): Point ={
    Point(row.getAs[Double]("longitude"), row.getAs[Double]("latitude"))
  }

  //updateCentroids implemented over pair RDDs
  //we obtain paired RDDs zipping cluster of the points with their longitude and latitude coordinates
  //and return array of centroids with the average longitude and latitude
  def updateCentroids(df: DataFrame): Array[(Double, Double, Int)] = {
    val rdd = df.select("longitude", "latitude").rdd.map(parsePoint)
    val keys = df.select("cluster").rdd
    val pairedRDD = keys.zip(rdd)
    val reduced = pairedRDD.mapValues((_, 1))
      .reduceByKey((row1, row2) => (
        Point(row1._1.longitude + row2._1.longitude, row1._1.latitude+ row2._1.latitude),
        row1._2+row2._2), 8)
      .mapValues{ case (row, count) =>
        Point(row.longitude / count, row.latitude / count)}
    reduced.collect().map(
      row => (row._2.longitude, row._2.latitude, row._1.getInt(0)))
  }
}

