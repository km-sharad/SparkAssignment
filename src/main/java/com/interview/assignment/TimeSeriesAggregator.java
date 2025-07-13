package com.interview.assignment;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.round;

/**
 * Usage: TimeSeriesAggregator <ts_input_dataset> <ts_output_dataset> <level>
 * level value: 1 = aggregation by date; 2 = aggregation by date and hour
 */

public final class  TimeSeriesAggregator {
	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder()
     		.appName("TSAggregator")
     		.getOrCreate();

		//Can be set to DEBUG, INFO, WARN, ERROR, depending upon the environment. Ideally set in log4j.properties file
		spark.sparkContext().setLogLevel("ERROR");

		//Necessary because the input file has timestamp in UTC
		spark.conf().set("spark.sql.session.timeZone", "UTC");

		if (args.length < 3) {
			System.out.println("Usage: TimeSeriesAggregator <ts_input_dataset> <ts_output_dataset> <level>");
			return;
		}
  		// Get the timeseries dataset filename
		String tsInDataFile = args[0];
		String tsOutDataDir = args[1];
		int level = Integer.parseInt(args[2]);

		StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Metric", DataTypes.StringType, false),
                DataTypes.createStructField("Value", DataTypes.DoubleType, false),
                DataTypes.createStructField("Timestamp", DataTypes.TimestampType, false)
        });		

		// Read the input file into a Spark DataFrame
		Dataset<Row> tsDf = spark.read()
			.option("header", "true")
			.schema(schema)
			.csv(tsInDataFile);

		if(level == 1) {
			aggByDate(tsDf, tsOutDataDir);
		}else {
			aggByDateHour(tsDf, tsOutDataDir);
		}

       // Stop the SparkSession 
       spark.stop();
	}

	private static void aggByDate(Dataset<Row> tsDf, String tsOutDataDir) {
		tsDf = tsDf
			.withColumn("MeasurementDate", to_date(col("Timestamp")))
			.drop("Timestamp");
					
		tsDf = tsDf
			.select("Metric", "Value", "MeasurementDate")
		    .groupBy("Metric", "MeasurementDate")
		    .agg(
				round(avg("Value"), 2).alias("AvgValue"),
				round(min("Value"), 2).alias("MinValue"),
				round(max("Value"), 2).alias("maxValue")
			)
		    .orderBy("Metric", "MeasurementDate");
   
   		// Show the resulting aggregations
		tsDf.show(5, false);
		System.out.println("Total Rows = " + tsDf.count());

		tsDf
			.write()
			.mode(SaveMode.Overwrite)
			.option("header", "true")
			.csv(tsOutDataDir);
	}

	private static void aggByDateHour(Dataset<Row> tsDf, String tsOutDataDir) {
		tsDf = tsDf
			.withColumn("MeasurementDate", to_date(col("Timestamp")))
			.withColumn("MeasurementHour", hour(col("Timestamp")))
			.drop("Timestamp");

		tsDf = tsDf
			.select("Metric", "Value", "MeasurementDate", "MeasurementHour")
		    .groupBy("Metric", "MeasurementDate", "MeasurementHour")
		    .agg(
				round(avg("Value"), 2).alias("AvgValue"),
				round(min("Value"), 2).alias("MinValue"),
				round(max("Value"), 2).alias("maxValue")
			)
		    .orderBy("Metric", "MeasurementDate", "MeasurementHour");
   
   		// Show the resulting aggregations
		tsDf.show(5, false);
		System.out.println("Total Rows = " + tsDf.count());	

		tsDf
			.write()
			.mode(SaveMode.Overwrite)
			.option("header", "true")
			.csv(tsOutDataDir);			
	}
}