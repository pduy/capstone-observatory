package observatory

import java.time.LocalDate

import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {
  val stationFile = "stations.csv"

  test("Fahrenheit degrees are not converted to celcius degrees") {
    def round2Decimals(d: Double): Double = BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    assert(round2Decimals(Extraction.toCelsius(-459.67)) == -273.15)
    assert(round2Decimals(Extraction.toCelsius(-50)) == -45.56)
    assert(round2Decimals(Extraction.toCelsius(0)) == -17.78)
    assert(round2Decimals(Extraction.toCelsius(10)) == -12.22)
    assert(round2Decimals(Extraction.toCelsius(20)) == -6.67)
    assert(round2Decimals(Extraction.toCelsius(110)) == 43.33)
    assert(round2Decimals(Extraction.toCelsius(50)) == 10)
  }

  test("Those items should be in the joined dataset") {
    val temperature75 = Extraction.locationYearlyAverageRecords(
      Extraction.locateTemperatures(1975, stationFile, "1975.csv"))

    assert(temperature75.toSeq
      .contains((Location.apply(44.359, -84.674), 6.866111111111116)))
  }
}