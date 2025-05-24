import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col}

object assignment1 {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("Assignment 1").master("local[*]").getOrCreate()

    val df: DataFrame=spark.read.option("header",value=true).option("inferSchema",value=true).csv("data/AAPL.csv")

    df.show(false)

    //df.withColumnRenamed("Open","open")

    val renamedColumns=List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("Close").as("close")
    )

    //It will pass entire sequence as into select block so to unpack use this : _*

    val stockData = df.select(renamedColumns: _*).withColumn("diff", col("close")-col("open")).filter(col("close") > col("open")*1.1)

    stockData.show(5,false)

  }

}
