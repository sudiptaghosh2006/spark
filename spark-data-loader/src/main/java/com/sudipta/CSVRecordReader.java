package com.sudipta;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
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
	    System.err.println("Usage: CSVRecordReader <file>");
	    System.exit(1);
	}
	logger.debug("File Name :::: " + args[0]);
	SparkConf sparkConf = new SparkConf().setAppName("CSVRecordReader").setMaster("local[2]");
	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	logger.debug("Spark Home ::::    " + sparkContext.getSparkHome().get());

	SparkSession sparkSession = new SparkSession(sparkContext.sc());
	Dataset<Row> dataset = sparkSession.read().option("header", true).option("inferSchema", true).csv(args[0]);
	dataset.show();
//	dataset.printSchema();


	int size = dataset.numericColumns().knownSize();

	Dataset<Row> datsetTotal = dataset.withColumn("TOTAL",
		dataset.col("Bengali").plus(dataset.col("English")).plus(dataset.col("History"))
			.$plus(dataset.col("Chemistry")).plus(dataset.col("Physics")).plus(dataset.col("Maths")));

//	datsetTotal.withColumn("Percent", functions.round(datsetTotal.col("TOTAL").$div(size), 2)).show();
	
	WindowSpec windowSpec = Window.orderBy(datsetTotal.col("TOTAL").desc());
	datsetTotal
	.withColumn("DenseRank",functions.dense_rank().over(windowSpec)) 
	.withColumn("Rank",functions.rank().over(windowSpec)) 
	.withColumn("RowNumber",functions.row_number().over(windowSpec)) 	
	.show();
	
	String debugString = datsetTotal.toJavaRDD().toDebugString();
	
	logger.debug("Data Lineage :::: \n" );
	logger.debug(debugString);
	
	
	

//	Dataset<Row> summaryRow = dataset.groupBy().max();
//	summaryRow.show();
//	dataset.union(summaryRow).show();

//	dataset.groupBy(null)

	Thread.sleep(100);
	sparkContext.close();
    }

}
