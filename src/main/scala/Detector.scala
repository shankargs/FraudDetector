import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Detector extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Fraud Detector")
    .config("spark.driver.memory", "3g")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val financeDF = spark.read.json("src/main/resources/finances-small.json")
  financeDF
    .na.drop("all")
    .na.fill("Unknown", Seq("Description"))
    .where($"Amount" =!= 0 || $"Description" === "Unknown")
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  if(financeDF.hasColumn("_corrupt_record")) {
    financeDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
      .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
  }

  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try
    def hasColumn(columnName: String): Boolean = Try(df(columnName)).isSuccess
  }

}
