package cn.lasagna.www.classifier.knn;

/**
 * Created by walkerlala on 16-10-23.
 */
import org.ansj.dic.LearnTool;
import org.ansj.domain.Term;
import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.Collections;
import java.util.*;

import cn.lasagna.www.classifier.ClassifierInterface;

public class KNN implements ClassifierInterface {
    private List<Map<String, String>> KnnSet;
    //TODO: this is temporary and would be moved to configuration file
    private int K = 100;
    private double keywordsWeight = 0.6;   // see `caculateJaccard()'

    public boolean buildTrainingModel(List<Map<String, String>> trainingSet) {
        //process the content of `keywords` tag and `description` tag
        this.KnnSet = new ArrayList<>();
        Map<String, String> newTuple;
        StringBuilder newKeywords;
        StringBuilder newDescription;
        String keywords;
        String description;
        List<Term> termList;
        LearnTool learn = new LearnTool();
        String termNameTrim;
        String termNatureStr;

        for (Map<String, String> tuple : trainingSet) {
            newTuple = new HashMap<>();
            newTuple.putAll(tuple);

            //parse `keywords` tag to generate words list
            keywords = tuple.get("keywords");
            termList = NlpAnalysis.parse(keywords, learn);
            new NatureRecognition(termList).recognition();
            newKeywords = new StringBuilder();
            for (Term term : termList) {
                try {
                    termNameTrim = term.getName().trim();
                    termNatureStr = term.getNatureStr();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }

                if (termNatureStr != "null") {
                    newKeywords.append(termNameTrim.toUpperCase() + ",");
                }
            }
            newTuple.replace("keywords", newKeywords.toString());

            // parse `descriptios` to generate clean words list
            description = tuple.get("description");
            termList = NlpAnalysis.parse(description, learn);
            newDescription = new StringBuilder();
            for (Term term : termList) {
                try {
                    termNameTrim = term.getName().trim();
                    termNatureStr = term.getNatureStr();
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }

                if (termNatureStr != "null") {
                    newDescription.append(termNameTrim.toUpperCase() + ",");
                }
            }
            newTuple.replace("description", newDescription.toString());

            this.KnnSet.add(newTuple);
        }

        return true;
    }

    public List<Map<String, String>> classify(List<Map<String, String>> dataSet) {
        List<Map<String, String>> KNearest;
        List<Map<String, String>> classifiedSet = new ArrayList<>();
        Map<String, String> newTuple;
        for (Map<String, String> tuple : dataSet) {
            newTuple = new HashMap<>();
            newTuple.putAll(tuple);
            KNearest = getKNearest(newTuple);
            String tag = voteForTag(KNearest);
            newTuple.put("tag", tag);  // this should change `dataSet`
            classifiedSet.add(newTuple);
        }
        return classifiedSet;
    }

    private List<Map<String, String>> getKNearest(Map<String, String> tuple) {
        //first sort the list in descending according to the distance to `tuple`(we use Jaccard coefficient here
        Collections.sort(this.KnnSet, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> o1, Map<String, String> o2) {
                // -1 -- less than, 1 -- greater than, 0 -- equal
                double o1Dist = calculateJaccard(o1, tuple);
                double o2Dist = calculateJaccard(o2, tuple);
                return o1Dist > o2Dist ? -1 : 1;
            }
        });

        List<Map<String, String>> KnnPart = new ArrayList<>();
        int round = (this.K <= this.KnnSet.size()) ? this.K : this.KnnSet.size();
        for (int i = 0; i < round; i++) {
            KnnPart.add(this.KnnSet.get(i));
        }
        return KnnPart;
    }

    private double calculateJaccard(Map<String, String> o1, Map<String, String> o2) {
        // we just calculate their distance according to the `keywords` tag and `description` tag
        List<String> kwList1 = Arrays.asList(o1.get("keywords").split(","));
        List<String> kwList2 = Arrays.asList(o2.get("keywords").split(","));
        List<String> descList1 = Arrays.asList(o1.get("description").split(","));
        List<String> descList2 = Arrays.asList(o2.get("description").split(","));
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

        return keywordsWeight * kwJaccard + (1 - keywordsWeight) * descJaccard;

    }

    private String voteForTag(List<Map<String, String>> KNearest) {
        Map<String, Integer> tags = new HashMap<>();
        for(Map<String, String> tuple : KNearest){
            String tag = tuple.get("tag");
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





