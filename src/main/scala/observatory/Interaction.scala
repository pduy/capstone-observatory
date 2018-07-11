package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.SparkSession

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  def splitHalf(tile: Tile): Array[Tile] ={
    Array(Tile(tile.x * 2, tile.y * 2, tile.zoom + 1),
      Tile(tile.x * 2 + 1, tile.y * 2, tile.zoom + 1),
      Tile(tile.x * 2, tile.y * 2 + 1, tile.zoom + 1),
      Tile(tile.x * 2 + 1, tile.y * 2 + 1, tile.zoom + 1)
    )
  }

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val n = math.pow(2.0, tile.zoom)
    val lon = tile.x * 360 / n - 180
    val lat = math.toDegrees(math.atan(math.sinh(math.Pi * (1 - tile.y * 2 / n))))

    Location(lat = lat, lon = lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val width = 256
    val height = 256

    val spark = SparkSession.builder()
      .appName("observatory")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val subTiles = spark.createDataset(splitHalf(tile)).flatMap(splitHalf)
      .flatMap(splitHalf)
      .flatMap(splitHalf)
      .flatMap(splitHalf)
      .flatMap(splitHalf)
      .flatMap(splitHalf)
      .flatMap(splitHalf)

    val tileLocations = subTiles.map(x => (x, tileLocation (x)))
    val predictedTemperatures = tileLocations.map(x => (x._1, Visualization.predictTemperature(temperatures, x._2)))
    val predictedColors = predictedTemperatures.map(x => (x._1, Visualization.interpolateColor(colors, x._2)))

    val pixels = predictedColors.map(c => (c._1, Pixel(r = c._2.red, g = c._2.green, b = c._2.blue, 127)))
      .collect()
      .sortWith((tile1, tile2) => {
        if (tile1._1.y == tile2._1.y)
          tile1._1.x < tile2._1.x
        else
          tile1._1.y < tile2._1.y
      })
      .map(_._2)

    println("length of pixels =  " + pixels.length)

    Image(width, height, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    val outMostTile = Tile(0, 0, 0)
    val maxZoom = 3

    def zoomNTimes(n: Int, tiles: Array[Tile]): Array[Tile] =
      if (n == 0) tiles else Array.concat(tiles, zoomNTimes(n - 1, tiles.flatMap(splitHalf)))

    yearlyData.foreach(pair => {
      val tiles = zoomNTimes(maxZoom, Array(outMostTile))
      tiles.foreach(t => generateImage(pair._1, t, pair._2))
    })

//    def makePath(year: Int, zoom: Int, x: Int, y: Int): String =
//      "target/temperatures/" + year + "/" + zoom + "/" + x + "-" + y + ".png"

  }

}
