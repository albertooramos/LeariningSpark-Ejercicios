import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Capitulo4 extends App {

  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "prueba")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  //A

  val flightsDF = spark.read.option("inferSchema","true").option("header","true")
    .csv("src/main/resources/departuredelays.csv")

  flightsDF.createTempView("us_delay_flights_tbl")

  spark.sql("SELECT * FROM us_delay_flights_tbl").show()


  //1

  val flightsModDF = flightsDF.select(substring($"date",2,2).as("day"),substring($"date",0,2).as("month")
    ,$"delay",$"distance",$"origin",$"destination")

  flightsModDF.show()

  //2


  spark.sql("""SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER by delay DESC""").show(15)

  val query1 = flightsDF.filter($"delay">120 && $"origin" === "SFO" && $"destination" === "ORD")
    .orderBy(desc("delay")).select("date","delay","origin","destination")
  query1.show()


  spark.sql("""SELECT delay, origin, destination,CASE WHEN delay > 360 THEN 'Very Long Delays' WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays' WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays' WHEN delay = 0 THEN 'No Delays' ELSE 'Early' END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)

  val query2 = flightsDF.withColumn("Flight_Delays", when($"delay"> 360,"Very Long Delays")
      .when($"delay" > 120,"Long Delays")
      .when($"delay" > 60,"Short Delays")
      .when($"delay" > 0,"Tolerable Delays")
      .otherwise("No delays")).select("delay","origin","destination","Flight_Delays")
    .orderBy($"origin",$"delay".desc)
  query2.show()

  //B


  //C

  spark.read.format("parquet").load("src/main/resources/outputs/firecalls_parquet").show(5)

  spark.read.format("avro").load("src/main/resources/outputs/firecalls_avro").show(5)

  spark.read.format("json").load("src/main/resources/outputs/firecalls_json").show(5)

  spark.read.option("inferSchema","true").option("header","true").csv("src/main/resources/outputs/firecalls_csv_1partition")
    .show(5)



}
