package cn.lasagna.www.classifier;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import cn.lasagna.www.util.*;
//import org.ansj.dic.LearnTool;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Preprocessor {

	private static HashMap<String, Integer> wordSample;   //hashmap: key-word, value-the number of sample that contains this word 
	private static int trainingSamples;  //the number of Samples in trainingSet
	public static int numOfTag = Configuration.numOfTag;
	public static String[] tags =  Configuration.tags;
	 MyLogger logger = new MyLogger(this.getClass());
	private DBUtil trainingSetDB = new DBUtil();
	private DBUtil dataSetDB = new DBUtil();
	private DBUtil tfidfDB = new DBUtil();
	
	public Preprocessor(){
		logger.info("Preprocessor initiating", MyLogger.STDOUT);
		wordSample = new HashMap<String, Integer>();
		trainingSamples = 0;
	}

	public void preprocess(){
		try {
			this.trainingSetDB.connectDB(Configuration.trainingSetDBUrl, Configuration.trainingSetDBUser,
					Configuration.trainingSetDBPasswd, Configuration.trainingSetDBName);
			logger.info("trainingSetDB connected. Going to load and preprocess trainingSet", MyLogger.STDOUT);

			this.tfidfDB.connectDB(Configuration.tfidfDBUrl, Configuration.tfidfDBUser,
					Configuration.tfidfDBPasswd, Configuration.tfidfDBName);
			logger.info("tfidfDB connected. Going to compute the tf-idf value for every single word", MyLogger.STDOUT);

			this.dataSetDB.connectDB(Configuration.sourceDBUrl, Configuration.sourceDBUser,
					Configuration.sourceDBPasswd, Configuration.sourceDBName);
			logger.info("dataSetDB connected. Going to load and preprocess trainingSet", MyLogger.STDOUT);
		} catch (Exception e){
			logger.info("Exception connecting database");
			e.printStackTrace();
			return;
		}

		if(preprocessTrainingDB()){
			logger.info("Successfully preprocess trainingSet and calculate TF-IDF", MyLogger.STDOUT);
		}else{
			logger.info("FAIL preprocess trainingSet", MyLogger.STDERR);
		}

		if(preprocessDataSetDB()){
			logger.info("Successfully preprocess dataSetDB", MyLogger.STDOUT);
		}else{
			logger.info("FAIL preprocess dataSetDB", MyLogger.STDERR);
		}

		try{
			trainingSetDB.closeDBConn();
			tfidfDB.closeDBConn();
			dataSetDB.closeDBConn();
		}catch (Exception e){
			logger.info("Exception closing database");
			e.printStackTrace();
		}
	}
	
	//preprocess the training set,
	//include: split the words,calculate the tf-idf value for different classifier
	private boolean preprocessTrainingDB(){
		String selectSQL = "SELECT * FROM `pages_table`";
		ResultSet selectRs;
		Statement selectSt;
		
		try{

			selectRs = trainingSetDB.query(selectSQL);
			selectSt = selectRs.getStatement();
			Record record;
			RecordPool dataPart = new RecordPool();
	
			selectRs.beforeFirst();  //set cursor to begining
			
			//load original database
			 while(selectRs.next()) {
				    trainingSamples ++;
	                record = new Record();
	                record.setPage_id(selectRs.getString("page_id"));
	                //record.setTag(selectRs.getString("tag"));
	                record.setTitle(generateWords(selectRs.getString("title")));
	                record.setKeywords(generateWords(selectRs.getString("keywords")));
	                record.setDescription(generateWords(selectRs.getString("description")));
				    //record.setNormal_content(generateWords(htmlBodyText(selectRs.getString("text"))));
	                dataPart.add(record);
			 }
			 
			 selectSt.close();
			 selectRs.close();
			 
			 //store the result of spliting words back into trainingSetDB
			 String table = "pages_table";
			 for(Record part : dataPart){
				 String id = part.getPage_id();
				 String title = part.getTitle();
				 String keywords = part.getKeywords();
				 String description = part.getDescription();
				 //String normal_content = part.getNormal_content();
				 String updateSQL = "UPDATE ? SET title = ?, keywords = ?, description = ? WHERE page_id = ? ";
				 trainingSetDB.modify(updateSQL, table, title, keywords, description, id);
			 }
			 
			this.computeTfidf(dataPart);

		}catch (Exception e){
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	private boolean preprocessDataSetDB() {
		String selectSQL = "select * from `pages_table`";
		ResultSet selectRs;
		Statement selectSt;

		try {
			selectRs = dataSetDB.query(selectSQL);
			selectSt = selectRs.getStatement();
			Record record;
			RecordPool dataPart = new RecordPool();

			selectRs.beforeFirst();

			while(selectRs.next()){
				record = new Record();
				record.setPage_id(selectRs.getString("page_id"));
				record.setTitle(generateWords(selectRs.getString("title")));
				record.setKeywords(generateWords(selectRs.getString("keywords")));
				record.setDescription(generateWords(selectRs.getString("description")));
				//record.setNormal_content(generateWords(htmlBodyText(selectRs.getString("text"))));
				dataPart.add(record);
			}
			selectSt.close();
			selectRs.close();

			String table = "pages_table";
			for(Record part : dataPart){
				String id = part.getPage_id();
				String title = part.getTitle();
				String keywords = part.getKeywords();
				String description = part.getDescription();
				//String normal_content = part.getNormal_content();
				String updateSQL = "UPDATE ? SET title = ?, keywords = ?, description = ? WHERE page_id = ?";
				dataSetDB.modify(updateSQL, table, title, keywords, description, id);
			}
		}catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
    private void computeTfidf(RecordPool rp) throws Exception{
    	try{
    		 tfidfDB.modify("CREATE TABLE IF NOT EXISTS tfidf_training (" +
                     "`word_id` int(20) NOT NULL AUTO_INCREMENT," +
                     "`page_id`  int(20) NOT NULL," +
                     "`word_value` varchar(100) NOT NULL," +
                     "`tf` double default 0.0," +
                     "`idf` double default 0.0," +
                     "`tf_idf` double default 0.0," +
                     "PRIMARY KEY (`word_id`)" +
                     ")ENGINE=InnoDB");
             tfidfDB.modify("truncate table `tfidf_training`");
             
             TfIdfPool TfIdfSet = new TfIdfPool();
             TfIdf newTI;
             String[] content;
             String[] content_remove_dul;
             HashMap<String, Integer>wordFre = new HashMap<String, Integer>();
             for(Record rc : rp){
            	 String title = rc.getTitle();
            	 String keywords = rc.getKeywords();
            	 String description = rc.getDescription();
				 //String normal_content = rc.getNormal_content();
            	 String document = title + keywords+ description;
            	 if(document.length()>=2){
            		 content = document.split(",");
                	 content_remove_dul = wordRemoval(content);
                	 for(String word : content_remove_dul){
             			if(wordSample.containsKey(word)) wordSample.put(word, wordSample.get(word)+1);
             			else wordSample.put(word, 1);
             		}
                 }
             }
             
             for(Record rc : rp){
            	 String page_id = rc.getPage_id();
            	 String title = rc.getTitle();
            	 String keywords = rc.getKeywords();
            	 String description = rc.getDescription();
	             //String normal_content = rc.getNormal_content();
            	 String document = title + keywords+ description;
            	 if(document.length()>=2){
            		 content = document.split(",");
                	 content_remove_dul = wordRemoval(content);
                	 for(String word : content){
                		 if(wordFre.containsKey(word)) wordFre.put(word, wordFre.get(word)+1);
                         else wordFre.put(word, 1);
                	 }
                	 for(String word : content_remove_dul){
                		 newTI = new TfIdf();
                		 int A = wordFre.get(word);
                		 int B = content.length;
                		 int C = trainingSamples;
                		 int D = wordSample.get(word);
                		 double tf = (double) A/B;
                		 double idf = Math.log((double)C/(D+1));
                		 double tf_idf = tf*idf;
                		 newTI.setPage_id(page_id);
                		 newTI.setWord_value(word);
                		 newTI.setWord_tf(tf);
                		 newTI.setWord_idf(idf);
                		 newTI.setWord_tf_idf(tf_idf);
                		 TfIdfSet.add(newTI);
                	 }
                	 wordFre.clear();
            	 }
             }
             
           //store the result of calculating tf-idf value  into tf-idf database
             String storeSQL = "INSERT INTO `tfidf_training`(word_id, page_id," +
                                             " word_value, tf, idf, tf_idf)" +
                                             " VALUES(?, ?, ? ,? ,? ,?) ";
             for(TfIdf part : TfIdfSet) {
                 tfidfDB.insert(storeSQL, part.getWord_id(),
                                            part.getPage_id(),
                                            part.getWord_value(),
                                            part.getWord_tf(),
                                            part.getWord_idf(),
                                            part.getWord_tf_idf());
             }
             
    	}catch(Exception e){
    		throw e;
    	}
    }
    
 // return word list seperate by ","
    public static String generateWords(String str){
        List<Term> termList = NlpAnalysis.parse(str).getTerms();

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
    
    //calculate every single word's frequency in a document
    public static HashMap<String, Double>getWordFre(String document){
    	document = generateWords(document);
		HashMap<String, Double> map = new HashMap<>();
		String[] content = document.split(",");
		for(String word : content){
			if(map.containsKey(word)) map.put(word, map.get(word)+1);
			else map.put(word, 1.0);
		}
    	return map;
    }
    
  //remove duplicated word in String Array
  	public static String[] wordRemoval(String[] content){
	    Set<String> s = new HashSet<>();
	    s.addAll(Arrays.asList(content));
	    return (String [])s.toArray();
  	}

	public static String htmlBodyText(String html){
		Document doc = Jsoup.parse(html);
		Element body = doc.body();
		//String text = body.text();
		return body.text();
	}
  	

}
