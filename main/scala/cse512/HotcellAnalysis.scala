package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.math.sqrt

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))


    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()
    pickupInfo.createOrReplaceTempView("pickupinfo")

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    //Step 1 : Forming a grid
    var gridCell = spark.sql("select x,y,z from pickupinfo where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
    gridCell.createOrReplaceTempView("gridCells")

    //Step 2 : Counting the number of points within the cell
    gridCell = spark.sql("select x,y,z,count(*) points from gridCells group by z,y,x order by z,y,x")
    gridCell.createOrReplaceTempView("hotSpots")
    gridCell.show()


    val cellPoints = spark.sql("select count(1) as numberOfCells,sum(points) as totalPoints from hotSpots")
    cellPoints.createOrReplaceTempView("cellPoints")

    spark.udf.register("square",(cellPoint:Int)=>((
      HotcellUtils.square(cellPoint)
      )))

    val squarePoints = spark.sql("select sum(square(points)) squares from hotSpots")
    squarePoints.createOrReplaceTempView("squarePoints") //xj^2

    val sumPoints = cellPoints.first().getLong(1).toDouble //summation of xj value

    val mean = (sumPoints/numCells.toDouble)

    val standardDeviation = sqrt((squarePoints.first().getDouble(0)/numCells.toDouble)-(mean.toDouble*mean.toDouble))

    spark.udf.register("computeNeighbourCellCount",(inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.computeNeighbourCellCount(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
    //To change adjacentCells function
    val neighbourCells = spark.sql("select computeNeighbourCellCount(hs1.x, hs1.y, hs1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as neighbourCellCount,"
      + "hs1.x as x, hs1.y as y, hs1.z as z, "
      + "sum(hs2.points) as hotCells "
      + "from hotSpots hs1,hotSpots hs2 "
      + "where (hs2.x = hs1.x+1 or hs2.x = hs1.x or hs2.x = hs1.x-1) "
      + "and (hs2.y = hs1.y+1 or hs2.y = hs1.y or hs2.y = hs1.y-1) "
      + "and (hs2.z = hs1.z+1 or hs2.z = hs1.z or hs2.z = hs1.z-1) "
      + "group by hs1.z, hs1.y, hs1.x "
      + "order by hs1.z, hs1.y, hs1.x")

    neighbourCells.createOrReplaceTempView("neighbourCells")


    spark.udf.register("computeGetisOrd",(neighbourCellCount: Int, hotCells: Int, numCells: Int, mean: Double, standardDeviation: Double) =>((HotcellUtils.computeGetisOrd(neighbourCellCount, hotCells, numCells,mean,standardDeviation))))


    val getisOrdInfo = spark.sql("select computeGetisOrd(neighbourCellCount,hotCells,"+numCells+","+mean+","+standardDeviation+") as getis,x,y,z from neighbourCells order by getis desc")
    getisOrdInfo.createOrReplaceTempView("getisOrd")


    val result = spark.sql("select x,y,z from getisOrd")
    result.createOrReplaceTempView("result")

    return result
  }
}
