package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  object StationColumns extends Enumeration {
    val STNIdentifier, WBANIdentifier, Latitude, Longitude = Value
  }

  object TemperatureColumns extends Enumeration {
    val STNIdentifier, WBANIdentifier, Month, Day, Temperature = Value
  }

  case class SparkLocalDate(year: Year, month: Int, day: Int) extends Serializable

  /**
    * @param fahrenheit   degrees in Fahrenheit
    * @return celsius     degrees in Celsius
    */
  def toCelsius(fahrenheit: Temperature): Temperature = (fahrenheit - 32) * 5.0 / 9.0

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val spark = SparkSession.builder()
      .appName("observatory")
      .master("local[*]")
//      .config("spark.executor.memory", 1500)
      .getOrCreate

    import spark.implicits._

//    Load station data, remove null rows on GPS, converting null STN and WBAN to empty string
    val stationDF = spark.read.csv(Paths.get(this.getClass.getResource("../" + stationsFile).toURI).toString)
      .filter((row: Row) => Option(row.get(2)).isDefined && Option(row.get(3)).isDefined)
      .map(row => (if (Option(row.get(0)).isDefined) row.getAs[String](0) else "",
        if (Option(row.get(1)).isDefined) row.getAs[String](1) else "",
        row.getAs[String](2).toDouble,
        row.getAs[String](3).toDouble))
      .toDF(StationColumns.values.toList.map(_.toString):_*)

//    Load temperature data, converting null STN and WBAN to empty string
    val temperatureDF = spark.read.csv(Paths.get(this.getClass.getResource("../" + temperaturesFile).toURI).toString)
      .map(row => (if (Option(row.get(0)).isDefined) row.getAs[String](0) else "",
        if (Option(row.get(1)).isDefined) row.getAs[String](1) else "",
        row.getAs[String](2).toInt,
        row.getAs[String](3).toInt,
        row.getAs[String](4).toDouble))
      .toDF(TemperatureColumns.values.toList.map(_.toString):_*)

    val joinedDataDf = stationDF.join(temperatureDF,
      usingColumns=List(StationColumns.STNIdentifier.toString, StationColumns.WBANIdentifier.toString),
      joinType = "inner")

    val joinedList = joinedDataDf.map(row =>
      (SparkLocalDate.apply(year, row.getAs[Int](4), row.getAs[Int](5)),
        Location.apply(row.getAs[Double](2), row.getAs[Double](3)),
        toCelsius(row.getAs[Double](6)))
    ).collect()

    joinedList.map(row => (LocalDate.of(row._1.year, row._1.month, row._1.day), row._2, row._3))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.groupBy(_._2).map(row => {
      val sizeOfTemperatures = row._2.size
      (row._1, row._2.map(_._3).sum / sizeOfTemperatures)
    })
  }

}
