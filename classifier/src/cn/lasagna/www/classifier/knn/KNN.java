package cn.lasagna.www.classifier.knn;

/**
 * Created by walkerlala on 16-10-23.
 */
import cn.lasagna.www.classifier.Record;
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.util.Configuration;
import org.ansj.dic.LearnTool;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.Collections;
import java.util.*;

import cn.lasagna.www.classifier.ClassifierInterface;

public class KNN implements ClassifierInterface {
    private RecordPool KnnSet;
    private int K = Configuration.K;
    private double keywordsWeight = Configuration.KNNKeywordsWeight;   // see `caculateJaccard()'

    public boolean buildTrainingModel(RecordPool trainingSet) {
        //process the content of `keywords` tag and `description` tag
        this.KnnSet = new RecordPool();
        Record newRecord;

        for (Record record : trainingSet) {
            newRecord = new Record();
            newRecord.putAll(record);

            //parse `keywords` tag to generate words list
            String keywords = record.getKeywords();
            String newKeywords = KNN.generateWords(keywords);
            newRecord.setKeywords(newKeywords);

            // parse `descriptios` to generate clean words list
            String description = record.getDescription();
            String newDescription = KNN.generateWords(description);
            record.setDescription(newDescription);

            this.KnnSet.add(record);
        }

        return true;
    }

    /* return word list seperate by . */
    public static String generateWords(String str){
        List<Term> termList = NlpAnalysis.parse(str).getTerms();

        //remove duplicate
        Set<Term> termSet = new HashSet<>();
        termSet.addAll(termList);
        termList.clear();
        termList.addAll(termSet);

        StringBuilder newStr = new StringBuilder();
        String termNameTrim;
        String termNatureStr;
        for(Term term:termList){
            try {
                termNameTrim = term.getName().trim();
                termNatureStr = term.getNatureStr();
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }

            // only those term which length is greater than 2 make sense
            // alternatively we can use a `removeStopWord()' function to
            // remove stop word such as ‘的', '得'，'了'...
            if(termNatureStr != "null" && termNameTrim.length() >= 2 && termNatureStr.contains("n")){
                newStr.append(termNameTrim.toUpperCase() + ",");
            }
        }

        return newStr.toString();
    }

    public RecordPool classify(RecordPool dataSet) {
        RecordPool KNearest;
        RecordPool classifiedSet = new RecordPool();
        Record newRecord;
        for (Record tuple : dataSet) {
            newRecord = new Record();
            newRecord.putAll(tuple);
            KNearest = getKNearest(newRecord);
            String tag = voteForTag(KNearest);
            newRecord.setTag(tag);  // this should change `dataSet`
            classifiedSet.add(newRecord);
        }
        return classifiedSet;
    }

    private RecordPool getKNearest(Record record) {
        //first sort the list in descending according to the distance to `tuple`(we use Jaccard coefficient here
        try {
            this.KnnSet.sort(new Comparator<Record>() {
                @Override
                public int compare(Record o1, Record o2) {
                    // -1 -- less than, 1 -- greater than, 0 -- equal
                    double o1Result = calculateJaccard(o1, record);
                    double o2Result = calculateJaccard(o2, record);
                    return o1Result > o2Result ? -1 : (o1Result < o2Result ? 1 : 0);
                }
            });
        }catch(IllegalArgumentException e){
            e.printStackTrace();
        }

        RecordPool KnnPart = new RecordPool();
        int round = (this.K <= this.KnnSet.size()) ? this.K : this.KnnSet.size();
        for (int i = 0; i < round; i++) {
            KnnPart.add(this.KnnSet.get(i));
        }
        return KnnPart;
    }

    private double calculateJaccard(Record o1, Record o2) {
        // we just calculate their distance according to the `keywords` tag and `description` tag
        List<String> kwList1 = Arrays.asList(o1.getKeywords().split(","));
        List<String> kwList2 = Arrays.asList(o2.getKeywords().split(","));
        List<String> descList1 = Arrays.asList(o1.getDescription().split(","));
        List<String> descList2 = Arrays.asList(o2.getDescription().split(","));
        Collections.sort(kwList1);
        Collections.sort(kwList2);
        Collections.sort(descList1);
        Collections.sort(descList2);
        int kwCommon = 0;
        int kwDiff1 = 0;
        int kwDiff2 = 0;
        for (String str : kwList1) {
            if (kwList2.contains(str)) {
                kwCommon++;
            } else {
                kwDiff1++;
            }
        }
        for (String str : kwList2) {
            if (!kwList1.contains(str))
                kwDiff2++;
        }
        double kwJaccard = kwCommon / (kwCommon + kwDiff1 + kwDiff2);

        int descCommon = 0;
        int descDiff1 = 0;
        int descDiff2 = 0;
        for (String str : descList1) {
            if (descList2.contains(str)) {
                descCommon++;
            } else {
                descDiff1++;
            }
        }
        for (String str : descList2) {
            if (!descList1.contains(str))
                descDiff2++;
        }
        double descJaccard = descCommon / (descCommon + descDiff1 + descDiff2);

        //return keywordsWeight * kwJaccard + (1 - keywordsWeight) * descJaccard;
        double result = keywordsWeight * kwJaccard + (1 - keywordsWeight) * descJaccard;
        /*
        System.out.println("======= Jaccard coefficient ===========");
        System.out.println("o1 kws:     " + o1.get("keywords"));
        System.out.println("o1 desc:    " + o1.get("description"));
        System.out.println("o2 kws:     " + o2.get("keywords"));
        System.out.println("o2 desc:    " + o2.get("description"));
        System.out.println("Jaccard Coefficient: " + result);
        System.out.println("======= END Jaccard ===========");
        */

        return result;

    }

    private String voteForTag(RecordPool KNearest) {
        Map<String, Integer> tags = new HashMap<>();
        for(Record record : KNearest){
            String tag = record.getTag();
            tags.merge(tag, 1, (a,b) -> a + b);
        }

        //sort by value to get the most vote
        List<Map.Entry<String, Integer>> list = new LinkedList<>(tags.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return -o1.getValue().compareTo(o2.getValue());
            }
        });
        return list.get(0).getKey();
    }
}





