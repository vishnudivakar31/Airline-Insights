package io.vdlabz;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Insight {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName(Insight.class.getName())
                .getOrCreate();

        Dataset<Row> csvData = spark
                .read()
                .option("inferSchema", true)
                .option("header", true)
                .csv("src/main/resources/csv/*");

        AnalyticsEngine analyticsEngine = new AnalyticsEngine(spark);

        analyticsEngine.findTopAirportsByTaxiTime(csvData);
        analyticsEngine.findTopAirlinesByDelay(csvData);
    }
}
