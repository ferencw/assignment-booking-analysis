import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf

object Main  extends App {

  Runner.run(args)

}
object Runner{
  def run(args: Array[String]): Unit = {
    if (args.length < 5 || args(0).isEmpty || args(1).isEmpty || args(2).isEmpty || args(3).isEmpty|| args(4).isEmpty) {
      println("Usage: Main <airports_path> <bookings_path> <start_date> <end_date> <output_path>")
      sys.exit(1)
    }

    val dfmt = new SimpleDateFormat("yyyy-MM-dd")
    val airports_path = args(0)
    val bookings_path = args(1)
    val output_path = args(4)

    val startDate = try {
      dfmt.parse(args(2))
    } catch {
      case e: Exception =>
        println("Error: Start date is not in the correct format (yyyy-MM-dd)")
        sys.exit(1)
    }
    
    val endDate = try {
      dfmt.parse(args(3))
    } catch {
      case e: Exception =>
        println("Error: End date is not in the correct format (yyyy-MM-dd)")
        sys.exit(1)
    }

    val startTimestamp = new java.sql.Timestamp(startDate.getTime)
    val endTimestamp = new java.sql.Timestamp(endDate.getTime)

    val spark = SparkSession.builder
      .appName("KLM Assignment")
      .getOrCreate()

    val airportColumns = Seq("airport_id", "name", "city", "country", "iata", "icao", "latitude", "longitude", "altitude", "timezone", "dst", "tz_database_time_zone", "type", "source")
    val airportDF = spark.read
      .option("header", "true")
      .csv(airports_path)
      .toDF(airportColumns: _*)

    val bookingsDF = spark.read.json(bookings_path)

    val netherlandsAirportsDF = airportDF
      .filter(col("country") === "Netherlands" && col("iata") =!= "\\N")
      .select("iata")

    val explodedDF = bookingsDF
      .select(explode(col("event.DataElement.travelrecord.productsList")).as("products"),
        col("event.DataElement.travelrecord.passengersList").as("passengers"))
      .select(col("passengers.uci").as("uci"),
        col("products.bookingStatus").as("bookingStatus"),
        col("products.flight.destinationAirport").as("destinationAirport"),
        col("products.flight.originAirport").as("originAirport"),
        col("products.flight.arrivalDate").as("arrivalDate"),
        col("products.flight.departureDate").as("departureDate"))
      .filter(col("bookingStatus") === "CONFIRMED" && col("arrivalDate") >= lit(startTimestamp) && col("arrivalDate") <= lit(endTimestamp))

    val onlyNetherlandsBookingsDF = explodedDF
      .join(netherlandsAirportsDF, explodedDF("originAirport") === netherlandsAirportsDF("iata"))
      .select("uci", "destinationAirport", "arrivalDate")

    val joinDF = onlyNetherlandsBookingsDF
      .join(airportDF, onlyNetherlandsBookingsDF("destinationAirport") === airportDF("iata"))
      .select(size(array_distinct(col("uci"))).as("no_passengers"), col("arrivalDate"), col("country"))

    val categoryAddedDF = joinDF
      .withColumn("dayOfWeek", dayofweek(col("arrivalDate")))
      .withColumn("dayName",
        when(col("dayOfWeek") === 1, "Monday")
          .when(col("dayOfWeek") === 2, "Tuesday")
          .when(col("dayOfWeek") === 3, "Wednesday")
          .when(col("dayOfWeek") === 4, "Thursday")
          .when(col("dayOfWeek") === 5, "Friday")
          .when(col("dayOfWeek") === 6, "Saturday")
          .when(col("dayOfWeek") === 7, "Sunday")
          .otherwise("Unknown"))
      .withColumn("season",
        when(month(col("arrivalDate")) >= 3 && month(col("arrivalDate")) <= 5, "Spring")
          .when(month(col("arrivalDate")) >= 6 && month(col("arrivalDate")) <= 8, "Summer")
          .when(month(col("arrivalDate")) >= 9 && month(col("arrivalDate")) <= 11, "Fall")
          .otherwise("Winter"))

    val windowSpec = Window.partitionBy("season", "dayName").orderBy(desc("sum_passengers"))
    val groupedDF = categoryAddedDF
      .groupBy("season", "dayName", "country")
      .agg(sum("no_passengers").as("sum_passengers"))
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .orderBy(desc("sum_passengers"))

    val outputDF = groupedDF.coalesce(1)

    outputDF.write
      .mode("overwrite")
      .csv(output_path + "top_countries")
  }
}