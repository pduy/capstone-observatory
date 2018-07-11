package observatory

import org.apache.spark.sql.SparkSession

import scala.collection.concurrent.TrieMap


/**
  * 4th milestone: value-added information
  */
object Manipulation {

  val gridTemperatures: TrieMap[Iterable[(Location, Temperature)], TrieMap[GridLocation, Temperature]] = TrieMap()
  val spark = SparkSession.builder()
    .appName("observatory")
    .master("local[*]")
    .getOrCreate()

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val currentGrid = gridTemperatures.getOrElseUpdate(temperatures, {TrieMap[GridLocation, Temperature]()})
    x: GridLocation => currentGrid.getOrElseUpdate(x, {
      Visualization.predictTemperature(temperatures, Location(lat = x.lat, lon = x.lon))
    })
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    x: GridLocation => {
      spark.sparkContext.parallelize(temperaturess.toList).map(t => makeGrid(t)(x)).sum * 1.0 / temperaturess.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    x: GridLocation => {
      makeGrid(temperatures)(x) - normals(x)
    }
  }


}

