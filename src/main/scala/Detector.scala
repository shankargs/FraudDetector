import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
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

  val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
    .orderBy($"Date").rowsBetween(-4,0)

  val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)

  financeDF
    .na.drop("all")
    .na.fill("Unknown", Seq("Description"))
    .where($"Amount" =!= 0 || $"Description" === "Unknown")
    .selectExpr("Account.Number as AccountNumber", "Amount",
      "to_date(CAST(unix_timestamp(Date, 'MM/dd/yyyy') AS TIMESTAMP)) AS date", "Description")
    .withColumn("rollingAverage", rollingAvgForPrevious4PerAccount)
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  if(financeDF.hasColumn("_corrupt_record")) {
    financeDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
      .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
  }

  financeDF.select(concat_ws(" ", $"Account.FirstName", $"Account.LastName")
    .as("FullName"), $"Account.Number".as("AccountNumber"))
    .distinct()
    .write.mode(SaveMode.Overwrite).json("Output/finances-small-json")

  financeDF.select($"Account.Number".as("AccountNumber"), $"Amount", $"Description", $"Date")
    .groupBy("AccountNumber")
    .agg(avg($"Amount").as("AvgTrans"), sum($"Amount").as("TotalTrans"), max($"Amount").as("MaxTrans"),
      min($"Amount").as("MinTrans"), count($"Amount").as("NumOfTrans"), stddev($"Amount").as("StdDevAmount"),
      collect_set($"Description").as("UniqueTrans"))
    .write.mode(SaveMode.Overwrite).json("Output/finances-small-account-details")

  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try
    def hasColumn(columnName: String): Boolean = Try(df(columnName)).isSuccess
  }
}