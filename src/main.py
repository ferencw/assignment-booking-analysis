from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, array_distinct, dayofweek, month, when, sum, desc, rank
from pyspark.sql.window import Window
from datetime import datetime
import argparse


def load_airport_data(file_location, spark):
    # Add header manually due to airport.dat csv didn't contain
    column_names = ['airport_id', 'name', 'city', 'country',
                    'iata', 'icao', 'latitude', 'longitude', 'altitude',
                    'timezone', 'dst', 'tz_database_time_zone', 'type', 'source']

    return spark.read.csv(file_location).toDF(*column_names)


def load_bookings_data(file_location, spark):
    return spark.read.json(file_location)


def main(airport_path: str, bookings_path: str, start_date, end_date, output: str):
    # Create a Spark session
    spark = SparkSession.builder.appName("KLM Assignment").getOrCreate()

    # Read airport_data
    airports_df = load_airport_data(airport_path, spark)
    bookings_df = load_bookings_data(bookings_path, spark)

    # Query all airports iata code in the Netherlands
    netherlands_airports_df = airports_df.filter(
        (airports_df.country == "Netherlands") & (airports_df.iata != "\\N")).select("iata")

    # Explode products to operate spark sql functions on those and filter results on criterias
    exploded_df = bookings_df.select(
        explode(col("event.DataElement.travelrecord.productsList")
                ).alias("products"),
        col("event.DataElement.travelrecord.passengersList").alias("passengers")) \
        .select(
            col("passengers.uci").alias("uci"),
            col("products.bookingStatus").alias("bookingStatus"),
            col("products.flight.destinationAirport").alias(
                "destinationAirport"),
            col("products.flight.originAirport").alias("originAirport"),
            col("products.flight.arrivalDate").alias("arrivalDate"),
            col("products.flight.departureDate").alias("departureDate")
    ).filter((col("bookingStatus") == "CONFIRMED") & (col("arrivalDate") >= start_period) & (col("arrivalDate") <= end_period))

    # Filter more to bookings only from the Netherlands, this is a good candidate for broadcast join
    only_netherlands_bookings_df = exploded_df.join(netherlands_airports_df, exploded_df.originAirport == netherlands_airports_df.iata) \
        .select("uci", "destinationAirport", "arrivalDate")

    # Join filtered bookings with airports table to retrieve country for destination airports, count unique passenger codes per flight
    join_df = only_netherlands_bookings_df.join(airports_df, only_netherlands_bookings_df.destinationAirport == airports_df.iata, "inner") \
        .select(size(array_distinct("uci")).alias("no_passengers"), "arrivalDate", "country")

    # Determine dayOfweek and season
    category_added_df = join_df.withColumn("dayOfWeek", dayofweek(col("arrivalDate"))) \
        .withColumn("dayName",
                    when(col("dayOfWeek") == 1, "Monday")
                    .when(col("dayOfWeek") == 2, "Tuesday")
                    .when(col("dayOfWeek") == 3, "Wednesday")
                    .when(col("dayOfWeek") == 4, "Thursday")
                    .when(col("dayOfWeek") == 5, "Friday")
                    .when(col("dayOfWeek") == 6, "Saturday")
                    .when(col("dayOfWeek") == 7, "Sunday")
                    .otherwise("Unknown")
                    ) \
        .withColumn("season",
                    when((month(col("arrivalDate")) >= 3) & (
                        month(col("arrivalDate")) <= 5), "Spring")
                    .when((month(col("arrivalDate")) >= 6) & (month(col("arrivalDate")) <= 8), "Summer")
                    .when((month(col("arrivalDate")) >= 9) & (month(col("arrivalDate")) <= 11), "Fall")
                    .otherwise("Winter"))

    # Group by per season per day of week per country
    grouped_df = category_added_df.groupBy("season", "dayName", "country").agg(sum("no_passengers").alias("sum_passengers")) \
        .select("sum_passengers", "country", "dayName", "season")

    # Calculate the max per country per season per day of week
    window_spec = Window.partitionBy(
        "season", "dayName").orderBy(desc("sum_passengers"))
    ranked_df = grouped_df.withColumn("rank", rank().over(window_spec))
    top_countries_df = ranked_df.filter(col("rank") == 1).drop(
        "rank").orderBy(desc("sum_passengers"))

    # The following is necessary for human readable results
    # Set the number of output partitions to 1
    output_df = top_countries_df.coalesce(1)

    # Write the DataFrame to a single CSV file
    output_df.write.mode("overwrite").csv(
        output + "top_countries", header=True)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Spark Application")
    parser.add_argument("--airport_data", required=True,
                        help="Input directory to airports data")
    parser.add_argument("--bookings_data", required=True,
                        help="Input directory to bookingsdata")
    parser.add_argument("--start_date", required=True,
                        help="Start date in YYYY-mm-dd")
    parser.add_argument("--end_date", required=True,
                        help="End date in YYYY-mm-dd")
    parser.add_argument("--output", required=True, help="The output directory")
    args = parser.parse_args()
  
    dfmt_str = "%Y-%m-%d"
    start_period = datetime.strptime(args.start_date, dfmt_str).date()
    end_period = datetime.strptime(args.end_date, dfmt_str).date()

    main(airport_path=args.airport_data, bookings_path=args.bookings_data, start_date=start_period,
         end_date=end_period, output=args.output)
