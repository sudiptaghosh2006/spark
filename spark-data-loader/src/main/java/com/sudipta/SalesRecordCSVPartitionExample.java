package com.sudipta;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.immutable.Seq;

public class SalesRecordCSVPartitionExample
{

    private static Logger logger = LoggerFactory.getLogger(SalesRecordCSVPartitionExample.class);


//    public static void main(String[] args) throws Exception
//    {
//	executeJob(args);
//    }
    
    private static void executeJob (String[] args)
    {

	int processors = Runtime.getRuntime().availableProcessors();
	logger.debug("processor count  :::: {}", processors);

	logger.debug("File Name :::: {}", args[0]);


	if (args.length < 1)
	{
	    logger.error("Usage: SalesRecordReader <file>");
	    System.exit(1);
	}

	SparkConf sparkConf = new SparkConf().setAppName("SalesRecordReader").setMaster("local[7]");

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
		
		dataset.show();
		
		dataset.select("*").where(" Country == 'Morocco' ").orderBy("Units Sold").show();
		
		List<Partition> partitions = dataset.toJavaRDD().partitions();
		logger.debug("RDD partition Count ::::   {} ", partitions.size());
		
		JavaRDD<Row> coalesce = dataset.toJavaRDD().coalesce(1);
		logger.debug("After Re partition Count ::::   {} ",coalesce.getNumPartitions());

		
		
		
//		Region: string (nullable = true)
//		 |-- Country: string (nullable = true)
//		 |-- Item Type: string (nullable = true)
//		 |-- Sales Channel: string (nullable = true)
//		 |-- Order Priority: string (nullable = true)
//		 |-- Order Date: string (nullable = true)
//		 |-- Order ID: integer (nullable = true)
//		 |-- Ship Date: string (nullable = true)
//		 |-- Units Sold: integer (nullable = true)
//		 |-- Unit Price: double (nullable = true)
//		 |-- Unit Cost: double (nullable = true)
//		 |-- Total Revenue: double (nullable = true)
//		 |-- Total Cost: double (nullable = true)
//		 |-- Total Profit: double (nullable = true)
		

//		int numericColumnCount = dataset.numericColumns().knownSize();
//		logger.debug("Numeric Column  count :::: {} ", numericColumnCount);

//		long rowCount = dataset.count();
//		logger.debug("Row  count :::: {} ", rowCount);
//		Dataset<Row> finalDataset = dataset.withColumn("INSERTED_ON", functions.current_timestamp());
		
//		Dataset<Row> partitionedData = dataset.withColumn("Partition_ID", functions.spark_partition_id());
		

//		finalDataset.groupBy("Country").df().show();

//		Dataset<Row> count = finalDataset.groupBy("Region", "Item Type").count();
//		count.orderBy("Region").show((int) count.count(), false);

//		logger.debug("row count {}", count.count());
		
//		Dataset<Row> count = partitionedData.groupBy("Partition_ID").count();
//		logger.debug("partition count {}", count.count());
//		count.show();
//		Dataset<Row> df = partitionedData.groupBy("Partition_ID").df();

		sparkContext.close();
	    }
	}
    
    }

}
