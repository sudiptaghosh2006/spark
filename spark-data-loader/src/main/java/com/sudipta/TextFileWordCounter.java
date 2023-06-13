package com.sudipta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileWordCounter
{

    private static Logger logger = LoggerFactory.getLogger(TextFileWordCounter.class);

//	public static void main(String[] args) throws Exception {
////		if (args.length < 1) {
////			System.err.println("Usage: JavaWordCount <file>");
////			System.exit(1);
////		}
//		logger.error("File Name :::: " + args[0]);
//		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
//		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//		JavaRDD<String> lines = ctx.textFile(args[0], 1);
////		JavaRDD<String> lines = ctx.textFile("C:\\Users\\SGHOSH43\\Desktop\\spark_example.txt", 1);
//
//		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//		JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
//		JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
//
//		List<Tuple2<String, Integer>> output = counts.collect();
//		for (Tuple2<?, ?> tuple : output) {
//			System.out.println(tuple._1() + ": " + tuple._2());
//		}
//		ctx.stop();
//	}

}
