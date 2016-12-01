package cn.lasagna.www.classifier.knn;

/**
 * Created by walkerlala on 16-10-23. 
 */
import cn.lasagna.www.classifier.Record;   
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.util.Configuration;
import cn.lasagna.www.util.DBUtil;
import cn.lasagna.www.util.MyLogger;
import java.util.Collections;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.CompareRecord;
import cn.lasagna.www.classifier.CompareRecordPool;
import cn.lasagna.www.classifier.Preprocessor;

public class KNN implements ClassifierInterface {
    private CompareRecordPool KnnSet;   //training_set
    private int K = Configuration.K;  //find K nearest
    private double keywordsWeight = Configuration.KNNKeywordsWeight;  
    private double titleWeight = Configuration.KNNTitleWeight;
    private double descriptionWeight = Configuration.KNNDescriptionWeght;
    
     MyLogger logger = new MyLogger(this.getClass());
    
    private DBUtil tfidfDB = new DBUtil();
    
    public KNN(){
    	logger.info("Classifier KNN initiating", MyLogger.STDOUT);
    	try {
            // connect training database
            this.tfidfDB.connectDB(Configuration.tfidfDBUrl, Configuration.tfidfDBUser,
                                          Configuration.tfidfDBPasswd, Configuration.tfidfDBName);

        } catch (Exception e){
            e.printStackTrace();
        }

    }
    
    public boolean buildTrainingModel(RecordPool trainingSet) {
        //process the content of `keywords` tag and `description` tag
        this.KnnSet = new CompareRecordPool();

        for (Record record : trainingSet) {
                CompareRecord comRecord = tfidfValue(record);               
            this.KnnSet.add(comRecord);
        }//end for

        return true;
    }
    
    private CompareRecord tfidfValue(Record record){ 
    	//从tfidf数据库中查出一组记录的一组tfidf值，再根据keyword、title、description建立HashMap后返回比较记录
    	String keywords = record.getKeywords();   
           String description = record.getDescription();
           String title = record.getTitle();
           CompareRecord comRecord = new CompareRecord();
           
           HashMap<String,Double> keywordMap = new HashMap<String,Double>();
           HashMap<String,Double> titleMap = new HashMap<String,Double>();
           HashMap<String,Double> descriptionMap = new HashMap<String,Double>();
         
    	String pageID = record.getPage_id();
    	String selectSQL = "SELECT * FROM  `tfidf_training` WHERE page_id = " + pageID;
    	ResultSet rs;
    	Statement rsStatement;
    	// load tfidf value into each hash map
    	try {
			rs = tfidfDB.query(selectSQL);
			rsStatement = rs.getStatement();
			rs.beforeFirst();
			while(rs.next()){
				String word = rs.getString("word_value");
				double tfidf_value = rs.getDouble("tf_idf");
				if( keywords.contains(word))
					keywordMap.put(word, tfidf_value);
				if(description.contains(word))
					descriptionMap.put(word, tfidf_value);
				if(title.contains(word))
					titleMap.put(word, tfidf_value);
					
			}//end while			
			rsStatement.close();
			rs.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("Can't select record value from tfidfDB. ",MyLogger.STDOUT);
			e.printStackTrace();
		}
    	
    	
    	//Calculate each vector length if the item exist, otherwise set the vector null
    	if (record.getDescription() != "")
    		descriptionMap.put("0", calculateVector(descriptionMap));
    	else
    		descriptionMap = null;
    	if(record.getKeywords() != "")
    		keywordMap.put("0",calculateVector(keywordMap));
    	else
    		keywordMap = null;
    	if(record.getTitle() != "")
    		titleMap.put("0", calculateVector(titleMap));
    	else
    		titleMap = null;
    	
    	//set compare record
		comRecord.setDescriptonMap(descriptionMap);
		comRecord.setKeywordMap(keywordMap);
		comRecord.setTitleMap(titleMap);
		comRecord.setPage_id(pageID);
		comRecord.setTag(record.getTag());

    	return comRecord;
    }
    
    private double calculateVector(HashMap<String,Double> map){
    	ArrayList<Double>  value = new ArrayList<Double>(map.values());
    	double sum = 0;
    	for( int i = 0, length = value.size(); i < length; i ++)
    		sum = sum + value.get(i);
    	
    	return Math.sqrt(sum);
    }
  
    public CompareRecord buildDataModel(Record record){
    	//use data set record build compare record, the value of hash map is tf value.(词频）
    	CompareRecord comRecord = new CompareRecord();
    	String pageID = record.getPage_id();
    	
    	//Paraphrases data sample use preprocessor class function
    	HashMap<String,Double> keywordMap = Preprocessor.getWordFre(record.getKeywords());   
           HashMap<String,Double> titleMap = Preprocessor.getWordFre(record.getTitle());
           HashMap<String,Double> descriptionMap = Preprocessor.getWordFre(record.getDescription());
    	
    	//Calculate and store vector length in order use it directly later
    	descriptionMap.put("0", calculateVector(descriptionMap));
    	keywordMap.put("0",calculateVector(keywordMap));
    	titleMap.put("0", calculateVector(titleMap));
    	
    	//set compare record
		comRecord.setDescriptonMap(descriptionMap);
		comRecord.setKeywordMap(keywordMap);
		comRecord.setTitleMap(titleMap);
		comRecord.setPage_id(pageID);
    	return comRecord;
      }
    
    public RecordPool classify(RecordPool dataSet) {
    	//classify a set of data at once
           CompareRecordPool KNearest;
           RecordPool classifiedSet = new RecordPool();
           Record dataRecord;
         
        for (Record tuple : dataSet) {
            dataRecord = new Record();
            dataRecord.putAll(tuple);
                 CompareRecord dataComRecord =  buildDataModel(dataRecord);
            //classify
            KNearest = getKNearest(dataComRecord);
                String tag = voteForTag(KNearest);
            
            dataRecord.setTag(tag);  // this should change `dataSet`
            classifiedSet.add(dataRecord);
        }
        return classifiedSet;
    }

    private CompareRecordPool getKNearest(final CompareRecord comDataRecord) {
        //first sort the list in descending according to the distance to `tuple`(we use cosine coefficient here
        try {
            this.KnnSet.sort( new Comparator<CompareRecord>() {
                	@Override
                public int compare(CompareRecord o1, CompareRecord o2) {
                    // -1 -- less than, 1 -- greater than, 0 -- equal
                    double o1Result = calculateCos(o1, comDataRecord);
                    double o2Result = calculateCos(o2, comDataRecord);
                    return o1Result > o2Result ? -1 : (o1Result < o2Result ? 1 : 0);
                }
            });
        }catch(IllegalArgumentException e){
            e.printStackTrace();
        }

        CompareRecordPool KnnPart = new CompareRecordPool();
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
    
    private double calculateCos(CompareRecord o1, CompareRecord o2){
    	double cos = 0.0;
    	double keyword_cos = compareMap(o1.getKeyword(), o2.getKeyword());
    	double description_cos = compareMap(o1.getDescripton(), o2.getDescripton());
    	double title_cos = compareMap(o1.getTitle(), o2.getTitle());
    	cos = keywordsWeight * keyword_cos + titleWeight * title_cos + descriptionWeight* description_cos;
    	return cos;
    }
    
    private double compareMap(HashMap<String,Double> map1, HashMap<String,Double> map2){ 	
    	if(  (map1 == null) || (map2 == null) )
    		return 0.0;
    	
    	double cos = 0;	
    	double map1_length = map1.get("0");
    	double map2_length = map2.get("0");
    	double similarity = 0;
    	for( String word : map1.keySet()){
    		if(word == "0")
    			continue;
    		if(map2.containsKey(word))
    			similarity = similarity + map1.get(word) * map2.get(word);
    	}
    	cos = similarity / (map1_length * map2_length);
    	return cos;
    }

    
    private String voteForTag(CompareRecordPool KNearest) {
        Map<String, Integer> tags = new HashMap<>();
        for(CompareRecord comRecord : KNearest){
        	String tag = comRecord.getTag();
            tags.merge(tag, 1, (a,b) ->  a + b);//统计tag数
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
