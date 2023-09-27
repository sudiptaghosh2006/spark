package com.sudipta;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesRecordCSVReader
{

    private static final String DB_ORDER_DATE = "ORDER_DATE";

    private static final String DB_ORDER_MONTH = "ORDER_MONTH";

    private static final String DB_ORDER_YEAR = "ORDER_YEAR";

    private static final String COLUMN_TOTAL_PROFIT = "Total Profit";

    private static final String COLUMN_TOTAL_COST = "Total Cost";

    private static final String COLUMN_TOTAL_REVENUE = "Total Revenue";

    private static final String COLUMN_UNIT_COST = "Unit Cost";

    private static final String COLUMN_UNIT_PRICE = "Unit Price";

    private static final String COLUMN_UNITS_SOLD = "Units Sold";

    private static final String COLUMN_SHIP_DATE = "Ship Date";

    private static final String COLUMN_ORDER_ID = "Order ID";

    private static final String COLUMN_ORDER_DATE = "Order Date";

    private static final String COLUMN_ORDER_PRIORITY = "Order Priority";

    private static final String COLUMN_SALES_CHANNEL = "Sales Channel";

    private static final String COLUMN_ITEM_TYPE = "Item Type";

    private static final String COLUMN_COUNTRY = "Country";

    private static final String COLUMN_REGION = "Region";

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

    private static void executeJob(String[] args) throws IOException
    {
	PropertiesConfiguration config =null;
	

	
	Parameters params = new Parameters();
	// Read data from this file
	File propertiesFile = new File("sales-reader-configuration.properties");

	FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
	    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
	    .configure(params.fileBased()
	        .setFile(propertiesFile));
	try
	{
	    config = (PropertiesConfiguration) builder.getConfiguration();
	    // config contains all properties read from the file
	}
	catch(ConfigurationException cex)
	{
	    // loading of the configuration file failed
	    cex.printStackTrace();
	}

	String fileLocation = config.getString("input-file-location");
	String fileName = config.getString("input-file-name");
	String inputFile = Paths.get(fileLocation, fileName).toString();
	
	
	int processors = Runtime.getRuntime().availableProcessors();
	logger.debug("Hadoop Home :::: {}", System.getenv("HADOOP_HOME"));
	logger.debug("processor count  :::: {}", processors);

	logger.debug("File Name :::: {}", inputFile);

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

	StructType referenceSchema = new StructType(
		new StructField[] { new StructField(COLUMN_REGION, DataTypes.StringType, false, Metadata.empty()),
			new StructField(COLUMN_COUNTRY, DataTypes.StringType, false, Metadata.empty()),
			new StructField(COLUMN_ITEM_TYPE, DataTypes.StringType, false, Metadata.empty()),
			new StructField(COLUMN_SALES_CHANNEL, DataTypes.StringType, false, Metadata.empty()),
			new StructField(COLUMN_ORDER_PRIORITY, DataTypes.StringType, false, Metadata.empty()),
			new StructField(COLUMN_ORDER_DATE, DataTypes.DateType, false, Metadata.empty()),
			new StructField(COLUMN_ORDER_ID, DataTypes.LongType, false, Metadata.empty()),
			new StructField(COLUMN_SHIP_DATE, DataTypes.DateType, false, Metadata.empty()),
			new StructField(COLUMN_UNITS_SOLD, DataTypes.IntegerType, false, Metadata.empty()),

			new StructField(COLUMN_UNIT_PRICE, DataTypes.FloatType, false, Metadata.empty()),
			new StructField(COLUMN_UNIT_COST, DataTypes.FloatType, false, Metadata.empty()),
			new StructField(COLUMN_TOTAL_REVENUE, DataTypes.FloatType, false, Metadata.empty()),
			new StructField(COLUMN_TOTAL_COST, DataTypes.FloatType, false, Metadata.empty()),
			new StructField(COLUMN_TOTAL_PROFIT, DataTypes.FloatType, false, Metadata.empty()),
//			new StructField("_corrupt_record", DataTypes.StringType, false, Metadata.empty())

		});
	SparkConf sparkConf = new SparkConf().setAppName("SalesRecordReader").setMaster("local[8]");
	sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

	try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf))
	{
	    logger.debug("Spark Home ::::   {} ", sparkContext.getSparkHome().get());
	    Configuration configuration = sparkContext.hadoopConfiguration();
	    FileSystem fileSystem = FileSystem.get(configuration);
	    fileSystem.setWriteChecksum(false);


	    try (SparkSession sparkSession = new SparkSession(sparkContext.sc()))
	    {

//		Dataset<Row> dataset = sparkSession.read().option("header", true).option("inferSchema", true)
//			.option("dateFormat", "MM/dd/yyyy").csv(args[0]);

		Dataset<Row> dataset = sparkSession.read().schema(referenceSchema).option("header", true)
//			.option("inferSchema", true)
			.option("dateFormat", "MM/dd/yyyy").option("dateFormat", "M/d/yyyy").csv(args[0]);

//		dataset.schema().fields().

		List<Partition> partitions = dataset.toJavaRDD().partitions();
		logger.debug("RDD partition Count ::::   {} ", partitions.size());

		dataset.printSchema();

//		int numericColumnCount = dataset.numericColumns().knownSize();
//		logger.debug("Numeric Column  count :::: {} ", numericColumnCount);

		long rowCount = dataset.count();
		logger.debug("Row  count :::: {} ", rowCount);
		Dataset<Row> finalDataset = dataset.withColumn("INSERTED_ON", functions.current_timestamp())
			.withColumn(DB_ORDER_YEAR, functions.year(dataset.col(COLUMN_ORDER_DATE)))
			.withColumn(DB_ORDER_MONTH, functions.month(dataset.col(COLUMN_ORDER_DATE)))
			.withColumn(DB_ORDER_DATE, functions.dayofmonth(dataset.col(COLUMN_ORDER_DATE)));

//		finalDataset.groupBy("Country").df().show();

//		Dataset<Row> groupedByDataset = finalDataset.groupBy(DB_ORDER_YEAR,COLUMN_REGION, COLUMN_ITEM_TYPE).count();
//		groupedByDataset.orderBy(DB_ORDER_YEAR,COLUMN_REGION).show((int) groupedByDataset.count(), false);
//
//		logger.debug("row count {}", groupedByDataset.count());
//		int count = (int)finalDataset.count();
//		finalDataset.show(count,false);

		StopWatch watch = new StopWatch();
		watch.start();

//		logger.debug("data will be saved in db  :::: {} ", rowCount); //
//		finalDataset.write().mode(SaveMode.Overwrite).jdbc(jdbcUrl, destinationTable, dbProps);
//		
		finalDataset = finalDataset.coalesce(1);

		logger.debug("data will be saved in file  :::: {} ", rowCount); //
		finalDataset.write().mode(SaveMode.Overwrite)
//		.option("header", true)
		.csv(config.getString("output-file-location-csv"));
//		.parquet(config.getString("output-file-location-parquet"));
//		.json(config.getString("output-file-location-json"));

//		finalDataset.write().mode(SaveMode.Overwrite).
//		format("avro").save("C:\\Users\\SGHOSH43\\Desktop\\SparkData\\OUTPUT\\avro\\namesAndFavColors.avro");

		watch.stop();
		long result = watch.getTime();
		logger.debug("Time taken  in ms  {}", result);
		logger.debug("finished data insert   :::: {} ", rowCount);

//		Thread.sleep(1000000);

//		finalDataset.select((finalDataset.col("Order Date"))).show();

//		Dataset<Row> select = finalDataset.select(functions.year(finalDataset.col("Order Date")).alias("year")
//			, 
//			functions.month(finalDataset.col("Order Date")).alias("dt_month")
//			,dayofmonth(elevDF.date).alias("dt_day"), 
//			dayofyear(elevDF.date).alias("dt_dayofy"), 
//			hour(elevDF.date).alias("dt_hour"), 
//			minute(elevDF.date).alias("dt_min"), 
//			weekofyear(elevDF.date).alias("dt_week_no"), 
//			unix_timestamp(elevDF.date).alias("dt_int")
//			);

//		select.show();

		Path homeDirectory = fileSystem.getHomeDirectory();		  
		logger.debug("Home Directory   :::: {} ", homeDirectory);
		Path pathSrc = new Path(inputFile);
		Path pathDest = new Path(config.getString("processed-file-location"));
		String[] split = fileName.split("\\.");
		String finalName = split[0].concat("_").concat(String.valueOf(System.currentTimeMillis())).concat(".").concat(split[1]);

		
		
		 String movedFile = Paths.get(pathDest.toString(), finalName).toString();
		 Path dstPath = new Path(movedFile);
		logger.debug(inputFile +" will be moved to    :::: {} ", movedFile);
		fileSystem.copyToLocalFile(pathSrc,dstPath );

		sparkContext.close(); // can not call this when deployed in cluster
	    }
	}

    }

}
