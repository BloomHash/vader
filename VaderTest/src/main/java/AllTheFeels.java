import com.google.common.collect.Iterables;
import com.vader.sentiment.analyzer.SentimentAnalyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class AllTheFeels{
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("AllTheFeels").setMaster("local");
        //SparkConf conf = new SparkConf().setAppName("AllTheFeels").setMaster("spark://raleigh:30216");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> tweets = sc.textFile(args[0]).repartition(10).persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<String> lines = sc.textFile(args[0]);


        //ArrayList<String> tweets = new ArrayList<String>();
        //lines.foreach(data -> {
        //    tweets.add(data);
       // });

        JavaPairRDD<String, Double> tweetScores = lines.flatMapToPair(
                s -> {
                    int numTweets = Iterables.size(Collections.singleton(s));
                    //List<Float> compounds = new ArrayList<Float>();
                    List<Tuple2<String, Double>> tweetScore = new ArrayList<>();
                    SentimentAnalyzer sa = new SentimentAnalyzer(s);
                    sa.analyze();
                    Map<String, Float> polar = sa.getPolarity();
                    for (Map.Entry<String, Float> entry : polar.entrySet()) {
                        if (entry.getKey().equals("compound")) {
                            tweetScore.add(new Tuple2<String, Double>(s, entry.getValue().doubleValue()));
                        }
                    }
                    return tweetScore.iterator();
                });
        JavaPairRDD<String, Integer> feelingsCounts = tweetScores.values().flatMapToPair(
                s -> {
                    int num_pos =0; int num_neg =0; int num_neu = 0;
                    List<Tuple2<String, Integer>> allFeels = new ArrayList<>();
                        if (s >= .05) {
                        num_pos++;
                    } else if (s <= -.05) {
                        num_neg++;
                    } else {
                        num_neu++;
                    }
                allFeels.add(new Tuple2<>("Positive tweets", num_pos));
                allFeels.add(new Tuple2<>("Negative tweets", num_neg));
                allFeels.add(new Tuple2<>("Neutral tweets", num_neu));
                return allFeels.iterator();
            }).reduceByKey((a,b) -> a+b);

        feelingsCounts.saveAsTextFile(args[1]);
        
        sc.stop();

    }
}
