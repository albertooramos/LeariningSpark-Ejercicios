import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Capitulo3 extends App {
  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "prueba")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  //A

  val rawFireCallsDF = spark.read.option("inferSchema","true").option("header","true")
    .csv("src/main/resources/sf-fire-calls.csv")

  val fireCallsDF = rawFireCallsDF.withColumn("CallDate",to_date($"CallDate","MM/dd/yyyy"))


  //Ejercicios libro

  // 1
  val fireCallsTypes2018 = fireCallsDF.filter(year($"CallDate") === 2018).select($"CallType").distinct()
  fireCallsTypes2018.show()




  // 2
  val fireCallsCountsByMonth2018 = fireCallsDF.filter(year($"CallDate")===2018).withColumn("Month",month($"CallDate"))
    .groupBy("Month").count().select("Month","count")

  fireCallsCountsByMonth2018.show()

  //3
  val topNbhoodFC2018= fireCallsDF.filter(year($"CallDate")===2018).groupBy($"Neighborhood").count()
    .select("Neighborhood").orderBy(desc("count")).limit(1)

  topNbhoodFC2018.show()

  // 4
  val worstResponseTime2018 = fireCallsDF.filter(year($"CallDate")===2018).groupBy($"Neighborhood").avg("Delay")
    .orderBy(desc("avg(Delay)")).limit(1)

  worstResponseTime2018.show()

  // 5
  val weekMostFireCalls2018 = fireCallsDF.filter(year($"CallDate") === 2018 ).withColumn("CallWeek",weekofyear($"CallDate"))
    .groupBy($"CallWeek").count().select("CallWeek","count").orderBy(desc("count")).limit(1)
  weekMostFireCalls2018.show()


  //B

  rawFireCallsDF.printSchema()

  //C

  //Que puede ser nulo o no.

  //D

  //E

  fireCallsDF.write.format("parquet")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_parquet")

  fireCallsDF.write.format("json")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_json")

  fireCallsDF.write.format("csv")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_csv")

  fireCallsDF.write.format("avro")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_avro")

  //F
  //ii.

  println(fireCallsDF.rdd.getNumPartitions)

  //iii.

  val fireCallsDF2 = fireCallsDF.repartition(1)

  //iv.

  fireCallsDF2.write.format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("src/main/resources/outputs/firecalls_csv_1partition")
}
