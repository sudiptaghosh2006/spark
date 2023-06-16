package com.sudipta;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVRecordReader
{

    private static Logger logger = LoggerFactory.getLogger(CSVRecordReader.class);

    public static void main(String[] args) throws Exception
    {
	executeJob(args);
    }

    private static void executeJob(String[] args) throws InterruptedException
    {
	if (args.length < 1)
	{
	    logger.error("Usage: CSVRecordReader <file>");
	    System.exit(1);
	}

	logger.debug("File Name :::: " + args[0]);
	SparkConf sparkConf = new SparkConf().setAppName("CSVRecordReader").setMaster("local[2]");
	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	logger.debug("Spark Home ::::    " + sparkContext.getSparkHome().get());

	StructType referenceSchema = new StructType(
		new StructField[] { new StructField("Student Name", DataTypes.StringType, false, Metadata.empty()),
			new StructField("DOB", DataTypes.DateType, false, Metadata.empty()),
			new StructField("Bengali", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("English", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("History", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Chemistry", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Physics", DataTypes.IntegerType, false, Metadata.empty()),
			new StructField("Maths", DataTypes.IntegerType, false, Metadata.empty()),
//			new StructField("_corrupt_record", DataTypes.StringType, false, Metadata.empty())

		});

	try
	{

	    SparkSession sparkSession = new SparkSession(sparkContext.sc());
	    Dataset<Row> dataset = sparkSession.read().schema(referenceSchema)
//		    .option("columnNameOfCorruptRecord","badRecords")
		    .option("mode", "PERMISSIVE")
//		    .option("mode", "DROPMALFORMED")
//		    .option("mode", "FAILFAST")
		    .option("header", true).option("dateFormat", "dd-MM-yyyy")
//		    .option("inferSchema", true)
		    .csv(args[0]);

//	dataset.show();
	    dataset.printSchema();
//	    dataset.select(functions.year(dataset.col("DOB"))).show();
//	    dataset.select(functions.year(dataset.col("DOB"))).show();
	    Dataset<Row> withColumn = dataset.withColumn("Year-of-birth",functions.year(dataset.col("DOB")));
		    //.filter(("Year-of-birth"));
	  
	    withColumn.filter(withColumn.col("Year-of-birth").between(2011, 2015))
	    .show();

//	int size = dataset.numericColumns().knownSize();

	    try
	    {
		dataset.filter(dataset.col("Bengali").equalTo(80)).toDF().show();

		Dataset<Row> datsetTotal = dataset.withColumn("TOTAL",
			dataset.col("Bengali").plus(dataset.col("English")).plus(dataset.col("History"))
				.plus(dataset.col("Chemistry")).plus(dataset.col("Physics"))
				.plus(dataset.col("Maths")));

//	datsetTotal.withColumn("Percent", functions.round(datsetTotal.col("TOTAL").$div(size), 2)).show();

		WindowSpec windowSpec = Window.orderBy(datsetTotal.col("TOTAL").desc());
		datsetTotal.withColumn("DenseRank", functions.dense_rank().over(windowSpec))
			.withColumn("Rank", functions.rank().over(windowSpec))
			.withColumn("RowNumber", functions.row_number().over(windowSpec)).show();

		String debugString = datsetTotal.toJavaRDD().toDebugString();
		logger.debug("Data Lineage :::: \n");
		logger.debug(debugString);

	    }
	    catch (Exception e)
	    {
		logger.debug("exception during reading file \n {}", args[0]);
	    }

//	Dataset<Row> summaryRow = dataset.groupBy().max();
//	summaryRow.show();
//	dataset.union(summaryRow).show();

//	dataset.groupBy(null)
	}
	catch (Exception e)
	{
	    logger.debug("exception details \n {}", e.getMessage());
	}

	Thread.sleep(100000);
	sparkContext.close();
    }

}
