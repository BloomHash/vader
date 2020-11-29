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


public class AllTheFeels2{
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
                    List<Tuple2<String, Double>> tweetScore = new ArrayList<>();
                    SentimentAnalyzer sa = new SentimentAnalyzer(s);
                    sa.analyze();
                    Map<String, Float> polar = sa.getPolarity();
                    for (Map.Entry<String, Float> entry : polar.entrySet()) {
                        if (entry.getKey().equals("compound")) {
                            //if has both, gets added to both (essentially gives both a +1 negative, neutral, or positive)
                            if (s.toLowerCase().contains("biden") && s.toLowerCase().contains("trump")){
                                tweetScore.add(new Tuple2<String, Double>("BIDEN:" + s, entry.getValue().doubleValue()));
                                tweetScore.add(new Tuple2<String, Double>("TRUMP:" + s, entry.getValue().doubleValue()));
                            }else if (s.toLowerCase().contains("biden") && !s.toLowerCase().contains("trump")){
                                tweetScore.add(new Tuple2<String, Double>("BIDEN:" + s, entry.getValue().doubleValue()));
                            }else if(!s.toLowerCase().contains("biden") && s.toLowerCase().contains("trump")){
                                tweetScore.add(new Tuple2<String, Double>("TRUMP:" + s , entry.getValue().doubleValue()));
                            } //else
                                //System.out.println("Neither: " + s);
                        }
                    }
                    return tweetScore.iterator();
                });
        /*tweetScores.foreach(data -> {
            System.out.println(data);
            });
        System.out.println("Tweets: " + tweetScores.count());*/

        JavaPairRDD<String, Integer> feelingsCounts = tweetScores.flatMapToPair(
                s -> {
                    int num_pos =0; int num_neg =0; int num_neu = 0;
                    int num_posB =0; int num_negB =0; int num_neuB = 0;

                    List<Tuple2<String, Integer>> tweetCounts = new ArrayList<>();

                    if (s._1.contains("TRUMP")) {
                        if (s._2 >= .05) {
                            num_pos++;
                        } else if (s._2 <= -.05) {
                            num_neg++;
                        } else {
                            num_neu++;
                        }
                    }
                   else {
                        if (s._2 >= .05) {
                            num_posB++;
                        } else if (s._2 <= -.05) {
                            num_negB++;
                        } else {
                            num_neuB++;
                        }
                    }
                   tweetCounts.add(new Tuple2<>("Trump Positive tweets", num_pos));
                   tweetCounts.add(new Tuple2<>("Trump Negative tweets", num_neg));
                   tweetCounts.add(new Tuple2<>("Trump Neutral tweets", num_neu));
                   tweetCounts.add(new Tuple2<>("Biden Positive tweets", num_posB));
                   tweetCounts.add(new Tuple2<>("Biden Negative tweets", num_negB));
                   tweetCounts.add(new Tuple2<>("Biden Neutral tweets", num_neuB));
                return tweetCounts.iterator();
            }).reduceByKey((a,b) -> a+b);

        feelingsCounts.saveAsTextFile(args[1]);

        sc.stop();

    }
}
