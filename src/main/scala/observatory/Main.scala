package observatory

import java.io.File
import java.time.Month

import org.apache.log4j.Logger

object Main extends App {

  Logger.getLogger("main app").trace("Loading data")
  val temperatureList = Extraction.locateTemperatures(year = 2000,
    stationsFile = "stations.csv",
    temperaturesFile = "2000.csv")


  Logger.getLogger("main app").trace("Grouping location data")
  val aggregatedByLocation = Extraction.locationYearlyAverageRecords(temperatureList)

  val colors = Iterable((60.asInstanceOf[Temperature], Color(255, 255, 255)),
    (32.asInstanceOf[Temperature], Color(255, 0,	0)),
    (12.asInstanceOf[Temperature], Color(255, 255,	0)),
    (0.asInstanceOf[Temperature], Color(0, 255, 255)),
    (-15.asInstanceOf[Temperature], Color(0, 0,	255)),
    (-27.asInstanceOf[Temperature], Color(255, 0, 255)),
    (-50.asInstanceOf[Temperature], Color(33, 0, 107)),
    (-60.asInstanceOf[Temperature], Color(0, 0, 0))
  )

  Logger.getLogger("main app").trace("Grouping location data")
  val image = Visualization.visualize(aggregatedByLocation, colors)
  image.output(new File("/home/duy/code/coursera/scala-capstone/observatory/plot.png"))
}
