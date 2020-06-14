package cse512

import scala.math.min
import scala.math.max

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val pointResult = pointString.split(",");

    val pointX = pointResult(0).toDouble
    val pointY = pointResult(1).toDouble

    val rectangleResult = queryRectangle.split(",");

    val rectangleX1 = rectangleResult(0).toDouble
    val rectangleY1 = rectangleResult(1).toDouble
    val rectangleX2 = rectangleResult(2).toDouble
    val rectangleY2 = rectangleResult(3).toDouble

    val minX = min(rectangleX1, rectangleX2)
    val maxX = max(rectangleX1, rectangleX2)

    val minY = min(rectangleY1, rectangleY2)
    val maxY = max(rectangleY1, rectangleY2)

    if(pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY)
      return true

    return false
  }

}
