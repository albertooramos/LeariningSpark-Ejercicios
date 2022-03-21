import org.apache.spark.sql.SparkSession

object Capitulo2a extends App {

  implicit val sparkSession = SparkSession.builder()
    .master(master = "local")
    .appName(name = "prueba")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val quijote = sparkSession.read.text("src/main/resources/quijote.txt")

  println(quijote.count())

  quijote.show()
  quijote.show(truncate=false)
  quijote.show(25)
  quijote.show(25,false)
  println(quijote.head(1))
  println(quijote.first())
  println(quijote.take(1))
}
