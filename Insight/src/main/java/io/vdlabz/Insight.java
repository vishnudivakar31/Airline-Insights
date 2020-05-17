package io.vdlabz;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.abs;

public class Insight {
    private static SparkSession spark;
    private static Dataset<Row> carrierCodes;
    private static Dataset<Row> airportCodes;
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName(Insight.class.getName())
                .getOrCreate();

        loadCarrierCodes("src/main/resources/support-data/carriers.csv");
        loadAirportCodes("src/main/resources/support-data/airports.csv");

        Dataset<Row> csvData = spark
                .read()
                .option("inferSchema", true)
                .option("header", true)
                .csv("src/main/resources/csv/*");

        findTopAirportsByTaxiTime(csvData);
        findTopAirlinesByDelay(csvData);
    }

    private static void loadAirportCodes(String filePath) {
        airportCodes = spark.read().option("header", true).option("inferSchema", true).csv(filePath);
        airportCodes = airportCodes
                .select(airportCodes.col("iata").as("airport-code"), airportCodes.col("airport").as("airport-name"));
    }

    private static void loadCarrierCodes(String filePath) {
        carrierCodes = spark.read().option("header", true).option("inferSchema", true).csv(filePath);
        carrierCodes = carrierCodes.withColumnRenamed("Code", "carrier-code");
        carrierCodes = carrierCodes.withColumnRenamed("Description", "airline-name");
    }

    private static void findTopAirlinesByDelay(Dataset<Row> csvData) {
        Dataset<Row> arrTimeDelay = csvData
                .filter(csvData.col("CRSArrTime").isNotNull().and(csvData.col("ArrTime").isNotNull()))
                .select(csvData.col("UniqueCarrier").as("carrier-code"),
                        abs(csvData.col("CRSArrTime").minus(csvData.col("ArrTime"))).as("delay"));

        Dataset<Row> depTimeDelay = csvData
                .filter(csvData.col("CRSDepTime").isNotNull().and(csvData.col("DepTime").isNotNull()))
                .select(csvData.col("UniqueCarrier").as("carrier-code"),
                        abs(csvData.col("CRSDepTime").minus(csvData.col("DepTime"))).as("delay"));

        Dataset<Row> data = arrTimeDelay.union(depTimeDelay);
        data = data.groupBy(data.col("carrier-code")).avg();
        data = data.withColumnRenamed("avg(delay)", "avg-delay");
        data = data.join(carrierCodes, "carrier-code");

        Dataset<Row> worstAirlines = data.orderBy(data.col("avg-delay").desc()).limit(10);
        Dataset<Row> topAirlines = data.orderBy(data.col("avg-delay").asc()).limit(10);

        worstAirlines.show();
        topAirlines.show();
    }

    private static void findTopAirportsByTaxiTime(Dataset<Row> csvData) {
        Dataset<Row> taxiOutData = csvData
                .filter(csvData.col("TaxiOut").isNotNull())
                .select(csvData.col("Origin").as("airport-code"), csvData.col("TaxiOut").as("taxi-time"));
        Dataset<Row> taxiInData = csvData
                .filter(csvData.col("TaxiIn").isNotNull())
                .select(csvData.col("Dest").as("airport-code"), csvData.col("TaxiIn").as("taxi-time"));
        Dataset<Row> data = taxiOutData.union(taxiInData);
        data = data.groupBy(data.col("airport-code")).avg();
        data = data.withColumnRenamed("avg(taxi-time)", "average-time");
        data = data.join(airportCodes, "airport-code");
        data = data.orderBy(data.col("average-time").desc());
        Dataset<Row> worstAirports = data.limit(10);
        Dataset<Row> bestAirports = data
                .orderBy(data.col("average-time").asc())
                .filter(data.col("average-time").notEqual("0.0"))
                .limit(10);
        System.out.println("Worst Airports");
        worstAirports.show();
        System.out.println("Best Airports");
        bestAirports.show();
    }
}
