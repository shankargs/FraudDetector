import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}


object DataFrameFunctions extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .appName("DfFunctions").master("local[*]")
    .getOrCreate()

  case class Person(fN: String, lN: String, age: Int, wtInLbs: Option[Double], jT: Option[String])
  val peopleDF = spark.createDataFrame(List(
    Person("Justin", "Pihony", 32, None, Some("Programmer")),
    Person("John", "Smith", 22, Some(176.7), None),
    Person("Jane ", "Doe", 62, None, None),
    Person(" jane", "Smith", 42, Some(125.3), Some("Chemical engg")),
    Person("John", "Doe", 25, Some(222.2), Some("Teacher"))
  ))
//  peopleDF.show(false)
  import spark.implicits._
  val correctedDF = peopleDF.withColumn("fN", trim(initcap($"fN")))
  correctedDF.sort($"wtInLbs".desc)
    .groupBy(lower($"fN")).agg(first($"wtInLbs", ignoreNulls = true)).show()

  correctedDF.sort($"wtInLbs".asc_nulls_last)
    .groupBy(lower($"fN")).agg(first($"wtInLbs", ignoreNulls = true)).show()

  correctedDF.filter(lower($"jT").isin(List("eng", "teacher"):_*)).show(false)
}
