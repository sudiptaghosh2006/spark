package com.sudipta;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.immutable.Seq;

public class SalesRecordDBReader
{

    private static Logger logger = LoggerFactory.getLogger(SalesRecordDBReader.class);

    private static String sourceTable = "dbo.SPARK_SALES_RECORD";
    private static String sourceTableQuery  = "( select * from dbo.SPARK_SALES_RECORD where [Country]='Finland' ) as T";
    private static String user = "user";
    private static String password = "password";

    private static String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=DemoDatabase;encrypt=false";
    private static String jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

//    public static void main(String[] args) throws Exception
//    {
//	executeJob(args);
//    }

    private static void executeJob(String[] args) throws InterruptedException
    {

	int processors = Runtime.getRuntime().availableProcessors();
	logger.debug("processor count  :::: {}", processors);

//	logger.debug("File Name :::: {}", args[0]);

	Properties dbProps = new Properties();
	dbProps.setProperty("connectionURL", jdbcUrl);
	dbProps.setProperty("driver", jdbcDriver);
	dbProps.setProperty("user", user);
	dbProps.setProperty("password", password);
	

	SparkConf sparkConf = new SparkConf().setAppName("SalesRecordReader").setMaster("local[3]");

	try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf))
	{
	    logger.debug("Spark Home ::::   {} ", sparkContext.getSparkHome().get());

	    try (SparkSession sparkSession = new SparkSession(sparkContext.sc()))
	    {

		StopWatch watch = new StopWatch();
		watch.start();

		logger.debug("data will be retrieved from db {} ",dbProps); //
		Dataset<Row> dataset = sparkSession.read()
			.option("numPartitions",2)
//			.option("numPartitions", 8)
			.option("partitionColumn", "Order ID")
			.option("lowerBound", 1).option("upperBound", 10000)
//			.jdbc(jdbcUrl, sourceTable, dbProps);
			.jdbc(jdbcUrl, sourceTableQuery, dbProps);
		

			
//		Dataset<Row> dataset =sparkSession.read().format("jdbc")
//			  .option("url", jdbcUrl)
//			  .option("dbtable", sourceTable)
//			  .option("user", user)
//			  .option("password", password)
//			  .load();

		List<Partition> partitions = dataset.toJavaRDD().partitions();
		logger.debug("RDD partition Count ::::   {} ", partitions.size());

		dataset.printSchema();

		int numericColumnCount = dataset.numericColumns().knownSize();
		logger.debug("Numeric Column  count :::: {} ", numericColumnCount);

		long rowCount = dataset.count();
		logger.debug(" Retrieved data  count :::: {} ", rowCount);
		dataset.show((int)dataset.count(),false );
		
//		dataset.write().csv("C:\\Users\\SGHOSH43\\Desktop\\SparkData\\OUTPUT\\csv\\500000SalesRecords.csv");
		
		watch.stop();
		logger.debug(" time taken  ==> {} (ms) ", watch.getTime());
		sparkContext.close();
	    }
	}

    }

}
