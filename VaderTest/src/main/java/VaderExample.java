import com.vader.sentiment.analyzer.SentimentAnalyzer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VaderExample {
    public static void main(String[] args) throws IOException {

        BufferedReader csvReader = new BufferedReader(new FileReader(args[0]));
        String row;
        ArrayList<String> tweets = new ArrayList<String>();

        while ((row = csvReader.readLine()) != null) {
                tweets.add(row);
        }
        csvReader.close();

        BufferedReader csvReader2 = new BufferedReader(new FileReader(args[1]));
        String row2;
        ArrayList<String> tweetsT = new ArrayList<String>();

        while ((row2 = csvReader2.readLine()) != null) {
            tweetsT.add(row2);
        }
        csvReader.close();


        /*
        ArrayList<String> sentences = new ArrayList<String>() {{

            add("Again. Read the damn article. It's all the same source:Some Trump guy said so on Facebook. Come on. Admit it. You don't have any facts.");
            add("Democrat here. After doing my own research and looking into this. I can tell you I know who Im voting for this year.. I used to be the fuck trump guy but not anymore. #StayWoke");
            add("It should be very clear to everyone with a brain that Benedict Comrade Trump is a murderer plain and simple. This diseased sewer rat sat back and did absolutely nothing when warned months ago. American blood is on");
            add("It wont matter how you vote Biden cant win. Get used to the idea of another four years its happening.");
            add("You forgot a vote for Biden = a vote for Trump");
            add("Wow which one of you would be the civil one was completely unpredictable how surprising.I dont think theres much incendiary about my statement. I believe Biden will lose the general. If that upsets you I dont know meditate for a bit and figure out your shit.");
            add("Benedict Traitor Trump is not just a national embarrassment he's a murderer as well. The Orange Dotard did nothing when he was advised months ago and now he's got the blood of all the Americans that are dying every ");
        }};*/

        int num_pos = 0;
        int num_neg = 0;
        int num_neu = 0;
        List<Float> compounds = new ArrayList<Float>();
        for (String sentence : tweets) {
            //System.out.println(sentence);
            SentimentAnalyzer sa = new SentimentAnalyzer(sentence);
            sa.analyze();
            Map<String, Float> polar = sa.getPolarity();
            //System.out.println(polar);

            for (Map.Entry<String, Float> entry : polar.entrySet()) {
               if (entry.getKey().equals("compound")){
                    compounds.add(entry.getValue());
                    if (entry.getValue()>=.05){
                        num_pos++;
                    }
                    if(entry.getValue()<=-.05){
                        num_neg++;
                    }
                    else{
                        num_neu++;
                    }
                }
            }
        }
        for (String sentence : tweets) {
            //System.out.println(sentence);
            SentimentAnalyzer sa = new SentimentAnalyzer(sentence);
            sa.analyze();
            Map<String, Float> polar = sa.getPolarity();
            //System.out.println(polar);

            for (Map.Entry<String, Float> entry : polar.entrySet()) {
                if (entry.getKey().equals("compound")){
                    compounds.add(entry.getValue());
                    if (entry.getValue()>=.05){
                        num_pos++;
                    }
                    if(entry.getValue()<=-.05){
                        num_neg++;
                    }
                    else{
                        num_neu++;
                    }
                }
            }
        }

        double sums = 0;
        for(int i =0; i < compounds.size(); i++){
            sums += compounds.get(i);
        }
        System.out.println("BIDEN");
        System.out.println("Compounds total = " + sums);
        double avg_score = sums/compounds.size();
        System.out.println("Average compounds = " + avg_score);
        System.out.println("Num pos: " + num_pos + " Num neg: " +num_neg + " Num neutral: " +num_neu);

        int num_posT = 0;
        int num_negT = 0;
        int num_neuT = 0;
        List<Float> compoundsT = new ArrayList<Float>();
        for (String sentence : tweetsT) {
            //System.out.println(sentence);
            SentimentAnalyzer saT = new SentimentAnalyzer(sentence);
            saT.analyze();
            Map<String, Float> polarT = saT.getPolarity();
            //System.out.println(polarT);

            for (Map.Entry<String, Float> entryT : polarT.entrySet()) {
                if (entryT.getKey().equals("compound")){
                    compoundsT.add(entryT.getValue());
                    if (entryT.getValue()>=.05){
                        num_posT++;
                    }
                    if(entryT.getValue()<=-.05){
                        num_negT++;
                    }
                    else{
                        num_neuT++;
                    }
                }
            }
        }
        double sumsT = 0;
        for(int i =0; i < compoundsT.size(); i++){
            sumsT += compoundsT.get(i);
        }
        System.out.println("TRUMP");
        System.out.println("Compounds total = " + sumsT);
        double avg_scoreT = sumsT/compoundsT.size();
        System.out.println("Average compounds = " + avg_scoreT);
        System.out.println("Num pos: " + num_posT + " Num neg: " +num_negT + " Num neutral: " +num_neuT);

    }
}
