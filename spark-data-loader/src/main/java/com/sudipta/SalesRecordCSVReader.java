package com.sudipta;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.immutable.Seq;

public class SalesRecordCSVReader
{

    private static Logger logger = LoggerFactory.getLogger(SalesRecordCSVReader.class);

    private static String destinationTable = "dbo.SPARK_SALES_RECORD";
    private static String user = "user";
    private static String password = "password";

    private static String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=DemoDatabase;encrypt=false";
    private static String jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    public static void main(String[] args) throws Exception
    {
	executeJob(args);
    }
    
    private static void executeJob (String[] args)
    {

	int processors = Runtime.getRuntime().availableProcessors();
	logger.debug("processor count  :::: {}", processors);

	logger.debug("File Name :::: {}", args[0]);

	Properties dbProps = new Properties();
	dbProps.setProperty("connectionURL", jdbcUrl);
	dbProps.setProperty("driver", jdbcDriver);
	dbProps.setProperty("user", user);
	dbProps.setProperty("password", password);

	if (args.length < 1)
	{
	    logger.error("Usage: SalesRecordReader <file>");
	    System.exit(1);
	}

	SparkConf sparkConf = new SparkConf().setAppName("SalesRecordReader").setMaster("local[3]");

	try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf))
	{
	    logger.debug("Spark Home ::::   {} ", sparkContext.getSparkHome().get());

	    try (SparkSession sparkSession = new SparkSession(sparkContext.sc()))
	    {

		Dataset<Row> dataset = sparkSession.read()
			.option("header", true)
			.option("inferSchema", true)
			.option("dateFormat","MM/dd/yyyy")
			.csv(args[0]);

		List<Partition> partitions = dataset.toJavaRDD().partitions();
		logger.debug("RDD partition Count ::::   {} ", partitions.size());

		dataset.printSchema();

		int numericColumnCount = dataset.numericColumns().knownSize();
		logger.debug("Numeric Column  count :::: {} ", numericColumnCount);

		long rowCount = dataset.count();
		logger.debug("Row  count :::: {} ", rowCount);
		Dataset<Row> finalDataset = dataset.withColumn("INSERTED_ON", functions.current_timestamp());

//		finalDataset.groupBy("Country").df().show();

		Dataset<Row> count = finalDataset.groupBy("Region", "Item Type").count();
		count.orderBy("Region").show((int) count.count(), false);

		logger.debug("row count {}", count.count());
//		int count = (int)finalDataset.count();
//		finalDataset.show(count,false);

		/*
		 * StopWatch watch = new StopWatch(); watch.start();
		 * 
		 * logger.debug("data will be saved in db  :::: {} ", rowCount); //
		 * finalDataset.write().mode(SaveMode.Append).jdbc(jdbcUrl, destinationTable,
		 * dbProps); watch.stop(); long result = watch.getTime();
		 * logger.debug("Time taken  in ms  {}", result);
		 * logger.debug("finished data insert   :::: {} ", rowCount);
		 */

//		Thread.sleep(1000000);
		
		
//		finalDataset.select(functions.year("Order Date"))
		
//		Dataset<Row> select = finalDataset.select(functions.year(finalDataset.col("Order Date")).alias("year")
//			, 
//			month(elevDF.date).alias('dt_month'), 
//			dayofmonth(elevDF.date).alias('dt_day'), 
//			dayofyear(elevDF.date).alias('dt_dayofy'), 
//			hour(elevDF.date).alias('dt_hour'), 
//			minute(elevDF.date).alias('dt_min'), 
//			weekofyear(elevDF.date).alias('dt_week_no'), 
//			unix_timestamp(elevDF.date).alias('dt_int')
//			);

//		select.show();
		
		sparkContext.close();
	    }
	}
    
    }

}
