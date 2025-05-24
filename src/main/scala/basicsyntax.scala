
import org.apache.spark.sql.functions.{col, concat, current_timestamp, expr, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
object basicsyntax {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Read File Example")
      .master("local[*]")  // Use local mode for testing
      .getOrCreate()

    val df:DataFrame=spark.read.option("header",value=true).option("inferSchema",value=true).csv("data/AAPL.csv")
    df.show(5,false)
    df.printSchema()

    import spark.implicits._

    //Select Columns
    //df.select("Date","Open","Close").show(5,false)
    df.select(col("Date"),$"Open",df("Close")).show(5,false)

    //filtering

    val column = df("Open")
    val newColumn = (column + 2.0).as("OpenIncreasedby2")
    val columnString = column.cast(StringType).as("OpenasString")

    df.select(column, newColumn,columnString).filter(newColumn > 2.0).filter(newColumn > column).filter(newColumn === column).show(5,false)


    //static value assignment and concat

    val litColumn = lit(2.0)
    val newcolumnString=concat(columnString, lit("Hello World"))

    df.select(column, newColumn,columnString,newcolumnString).show(5,false)

    //use show truncate = false or just false to view the whole string in spark

    //timestamp

    val timestampFromExpression = expr("cast(current_timestamp as string) as timestampExpressions")
    val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFunctions")

    df.select(timestampFromExpression,timestampFromFunctions).show(5,false)

    df.selectExpr("cast(Date as string)","Open+2.0 as val","current_timestamp()").show(5,false)

    df.createTempView("df")

    spark.sql("select * from df").show(5,false)



  }
}
