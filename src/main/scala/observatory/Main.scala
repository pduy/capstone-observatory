package observatory

import java.io.File
import java.time.Month

import org.apache.log4j.Logger

object Main extends App {

  Logger.getLogger("main app").trace("Loading data")
  val temperatureList = Extraction.locateTemperatures(year = 2015,
    stationsFile = "stations.csv",
    temperaturesFile = "2015.csv")


  Logger.getLogger("main app").trace("Grouping location data")
  val aggregatedByLocation = Extraction.locationYearlyAverageRecords(temperatureList)

  val colors = Iterable((60d, Color(255, 255, 255)),
    (32d, Color(255, 0,	0)),
    (12d, Color(255, 255,	0)),
    (0d, Color(0, 255, 255)),
    (-15d, Color(0, 0,	255)),
    (-27d, Color(255, 0, 255)),
    (-50d, Color(33, 0, 107)),
    (-60d, Color(0, 0, 0))
  )

  Logger.getLogger("main app").trace("Visualizing data")

  val temperature = Visualization.predictTemperature(aggregatedByLocation, Location(90, -180))
  println(temperature)
  val color = Visualization.interpolateColor(colors, temperature)
  print(color.red + " " + color.green + " " + color.blue)

  val image = Interaction.tile(aggregatedByLocation, colors, Tile(0, 0, 0))

//  val image = Visualization.visualize(aggregatedByLocation, colors)
  image.output(new File("/home/duy/code/coursera/scala-capstone/observatory/plot.png"))
}
