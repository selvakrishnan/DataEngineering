import org.apache.spark.sql.SparkSession

object CreateSparkSession extends App{

  val spark=SparkSession.builder().appName("Demo").master("local [*]").getOrCreate()

  println("Spark Session is Created")

}
