import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object BKMeans_df{

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("ksahatova-bkmeans-df")

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

  case class Point(longitude: Double, latitude: Double, cluster: Int)

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
    var df = data.select(col("pickup_longitude").as("longitude"),
      col("pickup_latitude").as("latitude"))
    df = df.withColumn("cluster", lit(1))
      .filter(col("longitude") =!= 0 && col("latitude") =!= 0)
    df.persist()

    println("*******Assign centroids using k-means for 2 clusters******")
    var m: Array[Point] = k_means(df, 2)
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
    df.unpersist()

    println("Centroids: ")
    println(m.mkString("\n"))
    println(s"****${dataset.toUpperCase} DATASET****")
    println(s"Execution time:  ${(System.currentTimeMillis() - startTime) / 1000}; " +
      s"number of iterations ${counter}; for k=${Kc} ")
  }

  def distance_bkm(df: DataFrame, m: Array[Point]): DataFrame = {
    val distances2centroids = udf((longitude: Double, latitude: Double, newClass: Int) => {
      val mi = m.filter(centroid => centroid.cluster == newClass).head
      distance(longitude, latitude, mi.longitude, mi.latitude)})
    val dfWithDistances = df.withColumn("distances",
      distances2centroids(df("longitude"), df("latitude"), df("cluster")))
    dfWithDistances.groupBy("cluster").agg(sum("distances").as("distance"))
  }

  def replace(m: Array[Point], k: Long, mForDividedCluster: Array[Point]): Array[Point] ={
    val replacedVal: Array[Point] = m.filter(m1 => m1.cluster != k) ++ mForDividedCluster
    var i = 0
    replacedVal.map(m1 => {
      i += 1
      m1.copy(cluster = i)
    })
  }

  def k_means(df: DataFrame, K: Int): Array[Point] = {
    val esp = 1.0E-9
    var continueLoop = true

    var m = sample(df, K)
    var X = assign(df, m)

    while (continueLoop) {
      val mUpdated = updateCentroids(X)
      X = assign(X, mUpdated)
      var sumLong = 0D
      var sumLat = 0D
      var i = 0
      m.foreach(row => {
        sumLong += Math.pow(row.longitude - mUpdated(i).longitude, 2)
        sumLat += Math.pow(row.latitude - mUpdated(i).latitude, 2)
        i = i + 1
      })
      m = mUpdated
      if ((sumLong <= esp * K) && (sumLat <= esp * K)){
        continueLoop = false
      }
    }
    m
  }

  def sample(df: DataFrame, K: Int): Array[Point] = {
    var i = 0
    val centroids = df.orderBy(rand()).limit(K)
    centroids.collect().map(x => {
      i = i + 1
      Point(x.getAs[Double]("longitude"), x.getAs[Double]("latitude"), i)
    })
  }

  def distance(long1: Double, lat1: Double, long2: Double, lat2: Double): Double = {
    Math.pow(long2 - long1, 2) + Math.pow(lat2 - lat1, 2)
  }

  def assign(X: DataFrame, m: Array[Point]): DataFrame ={
    val calcNewClass = udf((longitude: Double, latitude: Double) => {
      val array = m.map(centroid => {
        val distance2Class = distance(longitude, latitude, centroid.longitude, centroid.latitude)
        (centroid.longitude, centroid.latitude, centroid.cluster, distance2Class)
      })
      val newCentroid =   array.sortBy(_._4) // sort or orderBy
      newCentroid(0)._3 // return class of the min distance
    })
    X.withColumn("cluster", calcNewClass(X("longitude"), X("latitude")))
  }

  def updateCentroids(df: DataFrame): Array[Point] ={
    df.groupBy("cluster")
      .agg(avg("longitude").as("longitude"),
        avg("latitude").as("latitude"))
      .collect()
      .map(row => Point (row.getAs[Double]("longitude"),
        row.getAs[Double]("latitude"),
        row.getAs[Int]("cluster")))
  }
}

