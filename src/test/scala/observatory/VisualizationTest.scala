package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  test("Computed distance not correct") {
    val pairs = List(Location(lat = -45.87371703, lon = -38.49107883),
      Location(lat = -64.76871026, lon = 176.44104126),
      Location(lat = -38.78668771, lon = 12.12870186),
      Location(lat = 61.91281377, lon = 3.11810554),
      Location(lat = -15.13212365, lon = 179.27227232))

    val distanceBetweenConsecutiveLocations = List(
      0,
      0,
      7343.952986398561,
      8419.010324051273,
      11226.650760586701,
      14804.369757850991
    )

    pairs.zipWithIndex
      .flatMap(x => List(x, (x._1, x._2 + 1)))
      .groupBy(_._2)
      .map(x => {
        if (x._2.length > 1) {
          Visualization.getGreatCircleDistance(x._2.head._1, x._2.apply(1)._1)
        } else Visualization.getGreatCircleDistance(x._2.head._1, x._2.head._1)
      }).zip(distanceBetweenConsecutiveLocations)
      .foreach(x => assert(math.abs(x._1 -  x._2) < 1))
  }

  test("predictTemperature: given location with same longitude then predicted temperature at location z " +
    "should be closer to known temperature at location x than to known temperature at location y, if z is closer " +
    "(in distance) to x than y, and vice versa") {
    val longitude = 10
    val locationX = Location(70.0, longitude)
    val locationY = Location(-89.0, longitude)

    val locationZCloserToX = Location(locationX.lat - 5, longitude + 1)

    val locationZCloserToY = Location(locationY.lat + 5, longitude - 1)

    val xTemperature = 10.0

    val yTemperature = 20.0

    val givenTemperatureByPositionPairs = Iterable(
      (locationY, yTemperature),
      (locationX, xTemperature)
    )

    val predictedTemperatureOfALocationCloseToX = Visualization.predictTemperature(givenTemperatureByPositionPairs, locationZCloserToX)
    assert(math.abs(xTemperature - predictedTemperatureOfALocationCloseToX) < math.abs(yTemperature - predictedTemperatureOfALocationCloseToX))

    val actualTemperatureOfALocationCloseToY = Visualization.predictTemperature(givenTemperatureByPositionPairs, locationZCloserToY)
    assert(math.abs(yTemperature - actualTemperatureOfALocationCloseToY) < math.abs(xTemperature - actualTemperatureOfALocationCloseToY))
  }

  test("test color interpolation") {
    val arg0 = 46.38936709445329
    val arg1 = -82.32905760260174

    val aggregatedByLocation = Iterable((Location(45, -90), arg0),
      (Location(-45, 0), arg1))

    val colors = List(
      (arg0, Color(255, 0, 0)),
      (arg1, Color(0, 0, 255))
    )

    val predictedTemp = Visualization.predictTemperature(aggregatedByLocation, Location(45, -180))

    val color_close_to_arg0 = Visualization.interpolateColor(colors, predictedTemp)

    val first_distance = math.sqrt(math.pow(color_close_to_arg0.red - 255, 2)
      + math.pow(color_close_to_arg0.green, 2)
      + math.pow(color_close_to_arg0.blue, 2))
    val second_distance = math.sqrt(math.pow(color_close_to_arg0.red, 2)
      + math.pow(color_close_to_arg0.green, 2)
      + math.pow(color_close_to_arg0.blue - 255, 2))

    assert(first_distance < second_distance)
  }

}
