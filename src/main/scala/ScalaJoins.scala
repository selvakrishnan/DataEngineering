
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ScalaJoins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Scala Join Example")
      .master("local[*]")  // Use local mode for testing
      .getOrCreate()

    import spark.implicits._

    // Sample data for DataFrame1
    val data1 = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "David")
    )

    // Sample data for DataFrame2
    val data2 = Seq(
      (3, "HR"),
      (4, "Finance"),
      (5, "IT")
    )

    val df1 = data1.toDF("id", "name")
    val df2 = data2.toDF("id", "department")

    println("DataFrame 1:")
    df1.show()

    println("DataFrame 2:")
    df2.show()

    // Inner Join
    println("Inner Join:")
    df1.join(df2, Seq("id"), "inner").show()

    // Left Outer Join
    println("Left Outer Join:")
    df1.join(df2, Seq("id"), "left_outer").show()

    // Right Outer Join
    println("Right Outer Join:")
    df1.join(df2, Seq("id"), "right_outer").show()

    // Full Outer Join
    println("Full Outer Join:")
    df1.join(df2, Seq("id"), "outer").show()

    // Left Semi Join (Only rows from df1 that have a match in df2)
    println("Left Semi Join:")
    df1.join(df2, Seq("id"), "left_semi").show()

    // Left Anti Join (Only rows from df1 that *do not* have a match in df2)
    println("Left Anti Join:")
    df1.join(df2, Seq("id"), "left_anti").show()
    Thread.sleep(120000)
    spark.stop()
  }
}
