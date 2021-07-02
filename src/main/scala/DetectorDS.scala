import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Account(number: String, fN: String, lN: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double,
                       description: String)
case class TransactionForAverage(accountNumber: String, amount: Double, description: String,
                                 date: java.sql.Date)

object DetectorDS extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Fraud DetectorDS")
    .config("spark.driver.memory", "3g")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  val financesDS = spark.read.json("src/main/resources/finances-small.json")
    .withColumn("date", to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp"))).as[Transaction]

  val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
    .orderBy($"Date").rowsBetween(-4,0)

  val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)

  financesDS.na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
    .na.fill("Unknown", Seq("Description")).as[Transaction]
    .where($"Amount" =!= 0 || $"Description" === "Unknown")
    .select($"Account.Number".as("AccountNumber").as[String], $"Amount".as[Double], $"Date".as[java.sql.Date](Encoders.DATE),
    $"Description".as[String])
    .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  if(financesDS.hasColumn("_corrupt_record")) {
    financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
      .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
  }

  implicit class DatasetHelper[T](ds: Dataset[T]) {
    import scala.util.Try
    def hasColumn(columnName: String): Boolean = Try(ds(columnName)).isSuccess
  }

}
