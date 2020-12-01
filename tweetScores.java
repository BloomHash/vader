import com.vader.sentiment.analyzer.SentimentAnalyzer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class tweetScores {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("TweetScores").setMaster("local");
        //SparkConf conf = new SparkConf().setAppName("TweetScores").setMaster("spark://raleigh:30216");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]).persist(StorageLevel.MEMORY_AND_DISK());
        //JavaRDD<String> lines = sc.textFile(args[0]);

        String header = lines.first();
        JavaRDD<String> linesNoHeader = lines.filter(row -> !row.equals(header)).persist(StorageLevel.MEMORY_AND_DISK());
        JavaPairRDD<String, Tuple2<String, String>> tweets = linesNoHeader.mapToPair(s -> {
            String[] pieces = s.split(",", 2);
            System.out.println(pieces[0]);
            System.out.println(pieces[1]);
            String[] keywordSplit = pieces[1].split("\\[");
            //output= ID, (Tweet, [TAG])
            //System.out.println(keywordSplit[1]);
            //below line array out of bounds error:
            if (keywordSplit[1].equals("trump, biden]")){
                return new Tuple2<>(pieces[0], new Tuple2<>(keywordSplit[0].replaceAll("\\,",""), "[both]"));
            }
            return new Tuple2<>(pieces[0], new Tuple2<>(keywordSplit[0].replaceAll("\\,",""), "[" + keywordSplit[1]));
        }).repartition(20).persist(StorageLevel.MEMORY_AND_DISK());

        /*tweets.foreach(data -> {
           System.out.println(data);
         });
         */

        //output: (ID, (tweet, [TAG], score))
        JavaPairRDD<String, Tuple3<String, String,Double>> tweetScores = tweets.flatMapToPair(
                s -> {
                    List<Tuple2<String, Tuple3<String, String,Double>>> tweetScore = new ArrayList<>();
                    SentimentAnalyzer sa = new SentimentAnalyzer(s._2._1);
                    sa.analyze();
                    Map<String, Float> polar = sa.getPolarity();
                    for (Map.Entry<String, Float> entry : polar.entrySet()) {
                        if (entry.getKey().equals("compound")) {
                            //if has both, gets added to both (essentially gives both a +1 negative, neutral, or positive)
                            tweetScore.add(new Tuple2<String, Tuple3<String, String, Double>>(s._1, new Tuple3("," + s._2._1, s._2._2, entry.getValue().doubleValue())));
                        }
                    }
                    return tweetScore.iterator();
                });

        JavaRDD<String> tweetFile = tweetScores.flatMap(s ->{
            ArrayList<String> rowToString = new ArrayList<>();
            rowToString.add(s.toString().replaceAll("[()]", ""));
            return rowToString.iterator();
        });

        tweetFile.saveAsTextFile(args[1]);
        /*
        FileWriter csvWriter = new FileWriter("tweetsWithScores.csv");
        csvWriter.append("id,");
        csvWriter.append("place,");
        csvWriter.append("tweet,");
        csvWriter.append("keyword,");
        csvWriter.append("score");
        csvWriter.append("\n");
        //csvWriter.append(data.toString().replaceAll("[()]", ""));
        ArrayList<String> toCSV = new ArrayList<>();
        tweetScores.coalesce(1).foreach(data ->{
           toCSV.add(data.toString().replaceAll("[()]", ""));
        });

        for(String str: toCSV){
            csvWriter.append(str);
            csvWriter.append("\n");
        }

        csvWriter.flush();
        csvWriter.close();*/

        sc.stop();
    }
}