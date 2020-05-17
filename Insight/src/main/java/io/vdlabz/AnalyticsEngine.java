package io.vdlabz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.abs;

public class AnalyticsEngine {
    private final String CARRIER_FILE_PATH = "src/main/resources/support-data/carriers.csv";
    private final String AIRPORT_FILE_PATH = "src/main/resources/support-data/airports.csv";
    private final SparkSession spark;
    private Dataset<Row> carrierCodes;
    private Dataset<Row> airportCodes;

    public AnalyticsEngine(SparkSession spark) {
        this.spark = spark;
        loadAirportCodes();
        loadCarrierCodes();
    }

    private void loadAirportCodes() {
        airportCodes = spark.read().option("header", true).option("inferSchema", true).csv(AIRPORT_FILE_PATH);
        airportCodes = airportCodes
                .select(airportCodes.col("iata").as("airport-code"), airportCodes.col("airport").as("airport-name"));
    }

    private void loadCarrierCodes() {
        carrierCodes = spark.read().option("header", true).option("inferSchema", true).csv(CARRIER_FILE_PATH);
        carrierCodes = carrierCodes.withColumnRenamed("Code", "carrier-code");
        carrierCodes = carrierCodes.withColumnRenamed("Description", "airline-name");
    }

    public void findTopAirlinesByDelay(Dataset<Row> csvData) {
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

    public void findTopAirportsByTaxiTime(Dataset<Row> csvData) {
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
