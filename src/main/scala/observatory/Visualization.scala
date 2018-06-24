package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val earthRadius = 6371

//  val spark = SparkSession.builder()
//    .appName("observatory")
//    .master("local[*]")
//    .getOrCreate()

//  import spark.implicits._

  val (locCol, tempCol, colorCol) = ("Location", "Temperature", "Color")

  def getGreatCircleDistance(loc1: Location, loc2: Location): Double = {
    if (loc1.equals(loc2)) return 0.0
    else if (loc1.lat == -loc2.lat && math.abs(loc1.lon - loc2.lon) == 180) return math.Pi

    val deltaAlpha = math.acos(math.sin(math.toRadians(loc1.lat))
      * math.sin(math.toRadians(loc2.lat))
      + math.cos(math.toRadians(loc1.lat))
      * math.cos(math.toRadians(loc2.lat))
      * math.cos(math.abs(math.toRadians(loc1.lon) - math.toRadians(loc2.lon))))

    earthRadius * deltaAlpha
  }

  def weight(loc1: Location, loc2: Location, p: Double): Double = 1 / math.pow(getGreatCircleDistance(loc1, loc2), p)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
//    val tempDF = spark.createDataFrame(temperatures.toSeq).toDF(locCol, tempCol)
    // find somewhere inside 1km
    temperatures.foreach(x => {if (getGreatCircleDistance(x._1, location) < 1) return x._2})

    // if not, use invert distance weighting
    val sumTemp = temperatures.map(x => weight(x._1, location, 2) * x._2).sum
    val sumWeight = temperatures.map(x => weight(x._1, location, 2)).sum

    sumTemp / sumWeight
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    def interpolate(x0: Temperature, y0: Int, x1: Temperature, y1: Int, x: Temperature): Int =
      ((y0*(x1 - x) + y1*(x - x0)) / (x1 - x0)).toInt

//    val pointDF = spark.createDataFrame(points.toSeq).toDF(tempCol, colorCol)
    points.toSeq.sortBy(_._1)
      .zipWithIndex
      .flatMap(x => List(x, (x._1, x._2 + 1)))
      .groupBy(_._2)
      .map(group => {
        if (group._2.size == 2) {

          val tempTuple0 = group._2.toSeq.apply(0)._1
          val tempTuple1 = group._2.toSeq.apply(1)._1
          if (tempTuple0._1 <= value && value <= tempTuple1._1)
            Color(
              interpolate(tempTuple0._1,
                tempTuple0._2.red,
                tempTuple1._1,
                tempTuple1._2.red,
                value),
              interpolate(tempTuple0._1,
                tempTuple0._2.green,
                tempTuple1._1,
                tempTuple1._2.green,
                value),
              interpolate(tempTuple0._1,
                tempTuple0._2.blue,
                tempTuple1._1,
                tempTuple1._2.blue,
                value)
            )
          else -1
        } else -1
      })
      .filter(_.isInstanceOf[Color])
      .head.asInstanceOf[Color]
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val width = 360
    val height = 180

    def getRealCoordinate(loc: Location): Location = Location(lat=height/2 - loc.lat, lon=loc.lon - width/2)

    val pixels = (0 until width*height).map(index => {
      val currentLoc = Location(lat=index / width, lon=index % width)
      val predictedTemp = predictTemperature(temperatures, getRealCoordinate(currentLoc))
      interpolateColor(colors, predictedTemp)
    }).map(x => Pixel(x.red, x.green, x.blue, 255))

    Image(width, height, pixels.toArray)
  }

}

