package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.math.sqrt

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def computeNeighbourCellCount(inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    var xBoundary = false
    if(inputX == minX || inputX == maxX)
    {
      xBoundary = true
    }
    var yBoundary = false
    if(inputY == minY || inputY == maxY)
    {
      yBoundary = true
    }
    var zBoundary = false
    if(inputZ == minZ || inputZ == maxZ)
    {
      zBoundary = true
    }
    if(xBoundary && yBoundary && zBoundary)
    {
      return 7
    }
    if((xBoundary && yBoundary) || (xBoundary && zBoundary) || (yBoundary && zBoundary))
    {
      return 11
    }
    if(xBoundary || yBoundary || zBoundary)
    {
      return 17
    }
    return 26
  }

  def computeGetisOrd(neighbourCellCount: Int, hotCells: Int, numCells: Int, mean: Double, standardDeviation: Double): Double = {
    //val getisOrd = (hotCells.toDouble - (mean * neighbourCellCount.toDouble)) / (standardDeviation * (sqrt(((numberOfCells.toDouble * neighbourSquare.toDouble) - (neighbourCellCount.toDouble * neighbourCellCount.toDouble))) / (numberOfCells.toDouble - 1)))
    val getisOrd = (hotCells.toDouble - (mean * neighbourCellCount.toDouble)) / (standardDeviation * (sqrt(((numCells.toDouble * neighbourCellCount.toDouble) - (neighbourCellCount.toDouble * neighbourCellCount.toDouble)) / (numCells.toDouble - 1))))

    return getisOrd.toDouble
  }

  def square(value: Int): Double =
  {
    val square = value *value
    return square.toDouble
  }

}
