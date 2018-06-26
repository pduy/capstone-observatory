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

    val deltaAlpha =
      if (loc1.lat == -loc2.lat && math.abs(loc1.lon - loc2.lon) == 180) math.Pi
      else
        math.acos(
          math.sin(math.toRadians(loc1.lat)) * math.sin(math.toRadians(loc2.lat))
            + math.cos(math.toRadians(loc1.lat)) * math.cos(math.toRadians(loc2.lat))
            * math.cos(math.abs(math.toRadians(loc1.lon) - math.toRadians(loc2.lon))))

    earthRadius * deltaAlpha
  }

  def weight(loc1: Location, loc2: Location, p: Double): Double = 1.0 / math.pow(getGreatCircleDistance(loc1, loc2), p)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    // find somewhere inside 1km
    temperatures.foreach(x => {if (getGreatCircleDistance(x._1, location) < 1) return x._2})

    // if not, use invert distance weighting
    val weights = temperatures.map(x => weight(x._1, location, 2))
    val sumTemp = temperatures.map(_._2).zip(weights).map(x => x._1 * x._2).sum
    val sumWeight = weights.sum

    sumTemp / sumWeight
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    def interpolate(x0: Temperature, y0: Int, x1: Temperature, y1: Int, x: Temperature): Int =
      math.round((y0*(x1 - x) + y1*(x - x0)) / (x1 - x0)).toInt

//    val pointDF = spark.createDataFrame(points.toSeq).toDF(tempCol, colorCol)
    val (maxTemperature, maxColor) = points.maxBy(_._1)
    val (minTemperature, minColor) = points.minBy(_._1)

    if (value > maxTemperature) maxColor
    else if (value < minTemperature) minColor
    else {
      points.toSeq.sortBy(_._1)
        .zipWithIndex
        .flatMap(x => List(x, (x._1, x._2 + 1)))
        .groupBy(_._2)
        .map(group => {
          if (group._2.size == 2) {
            val (x0, y0) = group._2.head._1
            val (x1, y1) = group._2.apply(1)._1
            if (x0 <= value && value <= x1) {
              Color(
                red = interpolate(x0, y0.red, x1, y1.red, value),
                green = interpolate(x0, y0.green, x1, y1.green, value),
                blue = interpolate(x0, y0.blue, x1, y1.blue, value)
              )
            }
            else -1
          } else -1
        })
        .filter(_.isInstanceOf[Color])
        .head.asInstanceOf[Color]
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val spark = SparkSession.builder()
      .appName("observatory")
      .master("local[*]")
      .getOrCreate
    import spark.implicits._

    val width = 360
    val height = 180

    def getRealCoordinate(loc: Location): Location = Location(lat=height/2 - loc.lat, lon=loc.lon - width/2)

    val pixels = spark.createDataset(0 until width*height).map(index => {
      val currentLoc = Location(lat=index / width, lon=index % width)
      val predictedTemp = predictTemperature(temperatures, getRealCoordinate(currentLoc))
      interpolateColor(colors, predictedTemp)
    }).map(x => Pixel(r = x.red, g = x.green, b = x.blue, alpha =  255))

    Image(width, height, pixels.collect())
  }

}

