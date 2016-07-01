import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Splitter;

import scala.Tuple2;

public class Main
{
	public static void main(String[] args)
	{
		String inputString = args[0];
		String inputPath = args[1];
		String outputPathText = args[2];
		String outputPathWords = args[3];
		String outputPathMap = args[4];
		String outputPathReduce = args[5];
		
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame dfNews = sqlContext.read().json("hdfs://ldiag-master:9000/user/levenshtein" + inputPath + "/*");
		dfNews.registerTempTable("dfNews");
		
		DataFrame dfNewsText = sqlContext.sql("SELECT text FROM dfNews");
		
		JavaRDD<Row> rddNewsText = dfNewsText.javaRDD();

		//JavaRDD<String> textFile = sc.textFile("/home/andre/Desktop/Levenshtein/test.txt");
		
		JavaRDD<String> rawWords = rddNewsText.flatMap(new FlatMapFunction<Row, String>()
		{
			public Iterable<String> call(Row r) throws Exception {
				List<String> tempWords = Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(r.mkString().toLowerCase());
				return tempWords;
			}
		});
		
		/*JavaRDD<String> rawWords = textFile.flatMap(new FlatMapFunction<String, String>()
		{
			public Iterable<String> call(String s)
			{
				List<String> tempWords = Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(s);
				return tempWords;
				//return Arrays.asList(s.split(" "));
			}
		});*/
		
		JavaRDD<String> cleanWords = rawWords.flatMap(new FlatMapFunction<String, String>()
		{
			public Iterable<String> call(String s)
			{
				if (s.split(" ").length > 1)
					return Arrays.asList();
				
				if (StringUtils.containsAny(s, "'\"!@#$%¨&*()-_=+`´[]{}\\/~^º|,.<>;:?°“”‘’–0123456789\n\t  "))
					return Arrays.asList();
				else
					return Arrays.asList(s);
			}
		});
		
		JavaRDD<String> distinctWords = cleanWords.distinct();
		
		JavaPairRDD<Integer, String> pairsMap = distinctWords.mapToPair(new PairFunction<String, Integer, String>()
		{
			public Tuple2<Integer, String> call(String s)
			{
				int levDistance = Levenshtein.distance(inputString, s);
				  
				return new Tuple2<Integer, String>(levDistance, s);
			}
		});
		
		JavaPairRDD<Integer, String> pairsReduce = pairsMap.reduceByKey(new Function2<String, String, String>()
		{
			public String call(String a, String b)
			{
				return String.valueOf(a + ", " + b);
			}
		}).sortByKey();
		
		rddNewsText.saveAsTextFile("hdfs://ldiag-master:9000/user/levenshtein" + outputPathText);
		distinctWords.saveAsTextFile("hdfs://ldiag-master:9000/user/levenshtein" + outputPathWords);
		pairsMap.saveAsTextFile("hdfs://ldiag-master:9000/user/levenshtein" + outputPathMap);
		pairsReduce.saveAsTextFile("hdfs://ldiag-master:9000/user/levenshtein" + outputPathReduce);
		
		sc.close();
	}		
}
