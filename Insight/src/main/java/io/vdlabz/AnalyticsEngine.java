package io.vdlabz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

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

    public void findTopCancellationReasons(Dataset<Row> csvData) {
        StructField[] structFields = new StructField[] {
                new StructField("cancellation-code", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cancellation-reason", DataTypes.StringType, true, Metadata.empty())
        };

        StructType structType = new StructType(structFields);

        List<Row> cancellationCodeMapper = Arrays.asList(
                RowFactory.create("A", "Carrier Related"),
                RowFactory.create("B", "Weather Related"),
                RowFactory.create("C", "National Space and Security Related"),
                RowFactory.create("D", "Security Related")
        );
        Dataset<Row> cancellationCodeDS = spark.createDataFrame(cancellationCodeMapper, structType);

        Dataset<Row> data = csvData
                .filter(csvData.col("Cancelled").equalTo(true))
                .select(csvData.col("CancellationCode").as("cancellation-code"));
        data = data.withColumn("num", lit(1));
        data = data.groupBy(data.col("cancellation-code"))
                .sum()
                .withColumnRenamed("sum(num)", "total-counts")
                .orderBy(col("total-counts").desc());
        data = data.join(cancellationCodeDS, "cancellation-code");
        data.show();
    }

    public void findTopDelays(Dataset<Row> csvData) {
        Dataset<Row> carrierDelay = csvData.select(col("CarrierDelay").as("delay"));
        carrierDelay = carrierDelay.withColumn("delay-type", lit("carrier-delay"))
                .groupBy(col("delay-type")).avg()
                .withColumnRenamed("avg(delay)", "delay");

        Dataset<Row> weatherDelay = csvData.select(col("WeatherDelay").as("delay"));
        weatherDelay = weatherDelay.withColumn("delay-type", lit("weather-delay"))
                .groupBy(col("delay-type")).avg()
                .withColumnRenamed("avg(delay)", "delay");

        Dataset<Row> nasDelay = csvData.select(col("NASDelay").as("delay"));
        nasDelay = nasDelay.withColumn("delay-type", lit("nas-delay"))
                .groupBy(col("delay-type")).avg()
                .withColumnRenamed("avg(delay)", "delay");

        Dataset<Row> securityDelay = csvData.select(col("SecurityDelay").as("delay"));
        securityDelay = securityDelay.withColumn("delay-type", lit("security-delay"))
                .groupBy(col("delay-type")).avg()
                .withColumnRenamed("avg(delay)", "delay");

        Dataset<Row> lateAircraftDelay = csvData.select(col("LateAircraftDelay").as("delay"));
        lateAircraftDelay = lateAircraftDelay.withColumn("delay-type", lit("late-aircraft-delay"))
                .groupBy(col("delay-type")).avg()
                .withColumnRenamed("avg(delay)", "delay");

        Dataset<Row> data = carrierDelay
                .union(weatherDelay)
                .union(nasDelay)
                .union(securityDelay)
                .union(lateAircraftDelay)
                .orderBy(col("delay").desc());

        data.show();
    }
}
