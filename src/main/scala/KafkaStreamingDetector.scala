import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

object KafkaStreamingDetector extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[3]")
    .appName("Fraud Detector Streaming")
    .config("spark.driver.memory", "3g")
    .enableHiveSupport()
    .getOrCreate()

  case class Account(number: String, fN: String, lN: String)
  case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double,
                         description: String)
  case class TransactionForAverage(accountNumber: String, amount: Double, description: String,
                                   date: java.sql.Date)

  import spark.implicits._
  val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
    .orderBy($"Date").rowsBetween(-4,0)

  val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)

  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
  val kafkaStream = KafkaUtils.createDirectStream(streamingContext, "localhost:9092", "myflow1", Map("test" -> 1))

}
