import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Capitulo2b {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master(master = "local")
      .appName(name = "prueba")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val mnmFile = "src/main/resources/mnm.csv"

    val mnmDF = spark.read.option("header","true").option("inferSchema","true")
      .csv(mnmFile)

    val cntDF = mnmDF.groupBy($"State",$"Color").count()
      .select($"State",$"Color",$"count".as("Total"))
      .orderBy(desc("Total"))

    cntDF.show(50)
    println(s"Total Rows = ${cntDF.count()} \n")
    val calcntDF = mnmDF.where($"State"==="CA")
      .groupBy($"State",$"Color").count()
      .select($"State",$"Color",$"count".as("Total"))
      .orderBy(desc("Total"))
    calcntDF.show(20)

    //i.
    println("Extra exercises: \n")
    val maxcolorDF = mnmDF.groupBy($"Color").avg("Count").orderBy("avg(Count)")
    maxcolorDF.show()

    //ii.
    val nvDF = mnmDF.where($"State" === "NV").groupBy($"State",$"Color").max("Count")
    nvDF.show()

    //iii.
    val aggDF = mnmDF.groupBy($"State",$"Color")
      .agg(min("Count").as("min_amount"),avg("Count").as("avg_amount"),max("Count").as("max_amount"))
      .orderBy("State")
    aggDF.show()

    //iv.
    mnmDF.createTempView("sweets")
    spark.sql("SELECT DISTINCT state FROM sweets ORDER BY state").show()
    spark.stop()

  }
}
