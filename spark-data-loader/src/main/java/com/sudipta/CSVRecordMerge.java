package com.sudipta;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVRecordMerge
{

    private static Logger logger = LoggerFactory.getLogger(CSVRecordMerge.class);

//    public static void main(String[] args) throws Exception
//    {
//	executeJob(args);
//    }

    private static void executeJob(String[] args) throws InterruptedException
    {

	SparkConf sparkConf = new SparkConf().setAppName("CSVRecordMerge")
//		.setMaster("spark://10.48.249.226:7077");
		.setMaster("local[2]");
	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	logger.debug("Spark Home ::::    " + sparkContext.getSparkHome().get());

	StructType masterReferenceSchema = new StructType(
		new StructField[]
		{ 
		        new StructField("RegID", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Student Name", DataTypes.StringType, false, Metadata.empty()),			
			new StructField("DOB", DataTypes.DateType, false, Metadata.empty()),
			new StructField("Bengali", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("English", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("History", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Chemistry", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Physics", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Maths", DataTypes.IntegerType, false, Metadata.empty())

		});
	
	
	StructType phoneNoReferenceSchema = new StructType(
		new StructField[] 
		{ 
			new StructField("RegID", DataTypes.IntegerType, false, Metadata.empty()),			
			new StructField("Phone No", DataTypes.IntegerType, false, Metadata.empty())

		});

	try
	{

	    SparkSession sparkSession = new SparkSession(sparkContext.sc());
	    Dataset<Row> masterDataset = sparkSession.read().schema(masterReferenceSchema)
		    .option("mode", "PERMISSIVE")
//		    .option("mode", "DROPMALFORMED")
//		    .option("mode", "FAILFAST")
		    .option("header", true).option("dateFormat", "dd-MM-yyyy")
		    .csv("C:\\Users\\SGHOSH43\\Desktop\\SparkData\\INPUT\\PriyashaProject_Master.csv");

	    masterDataset.show();
	    
	    Dataset<Row> phoneNumberDataset = sparkSession.read().schema(phoneNoReferenceSchema)
		    .option("mode", "PERMISSIVE")
		    .option("header", true)
		    .csv("C:\\Users\\SGHOSH43\\Desktop\\SparkData\\INPUT\\PriyashaProjectPhoneNo.csv");

	    phoneNumberDataset.show(); 
	    Dataset<Row> mergedDataset = masterDataset
//		    .join(phoneNumberDataset, masterDataset.col("RegID").equalTo(phoneNumberDataset.col("RegID")),"left");
//	    	    .join(phoneNumberDataset, masterDataset.col("RegID").equalTo(phoneNumberDataset.col("RegID")),"right");
	    	    .join(phoneNumberDataset, masterDataset.col("RegID").equalTo(phoneNumberDataset.col("RegID")),"inner");
//	    	    .join(phoneNumberDataset, masterDataset.col("RegID").equalTo(phoneNumberDataset.col("RegID")),"full");
	    
	    mergedDataset.show((int) mergedDataset.count());
	    }
	    catch (Exception e)
	    {
		logger.debug("exception during reading file \n {}", args[0]);
	    }



	Thread.sleep(100);
	sparkContext.close();
    }

}
