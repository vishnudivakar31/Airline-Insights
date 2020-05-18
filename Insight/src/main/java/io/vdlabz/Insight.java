package io.vdlabz;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Insight {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        if(args.length < 3) {
            throw new Exception("Expecting 2 arguments, \n1: Input Directory\n2. Output Directory");
        }

        String inputDirectory = args[0];
        String outputDirectory = args[1];
        String supportDirectory = args[2];

        SparkSession spark = SparkSession
                .builder()
                .master("yarn-cluster")
                .appName(Insight.class.getName())
                .getOrCreate();

        Dataset<Row> csvData = spark
                .read()
                .option("inferSchema", true)
                .option("header", true)
                .csv(String.format("%s/*", inputDirectory));

        AnalyticsEngine analyticsEngine = new AnalyticsEngine(spark, outputDirectory, supportDirectory);

        analyticsEngine.findTopAirportsByTaxiTime(csvData);
        analyticsEngine.findTopAirlinesByDelay(csvData);
        analyticsEngine.findTopCancellationReasons(csvData);
        analyticsEngine.findTopDelays(csvData);
        analyticsEngine.findTopAirlinesByFleet(csvData);
    }
}
