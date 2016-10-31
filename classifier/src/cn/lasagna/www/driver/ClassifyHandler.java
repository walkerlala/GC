package cn.lasagna.www.driver;

/**
 * Created by walkerlala on 16-10-24.
 */
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.knn.KNN;
import cn.lasagna.www.util.Configuration;
import cn.lasagna.www.util.DBUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import org.apache.log4j.Logger;

/* TODO: should rebuild ambiguity.dic */
public class ClassifyHandler {
    //Without any access qualifier(public, private, protected)desclared, logger has "package visibility"(default visibility in Java)
    //In Java there are public, protected, package (default), and private visibilities; ordered from most visible to least.
    Logger logger = Logger.getLogger(this.getClass());

    private String classiferName;
    private ClassifierInterface classifer;
    private List<Map<String, String>> trainingSet;

    private DBUtil trainingSetDB = new DBUtil();
    private DBUtil dataSetDB = new DBUtil();
    private DBUtil resultSetDB = new DBUtil();

    public ClassifyHandler(){
        logger.info("ClassifierHandler initiating");
        System.out.println("ClassifierHandler initiating");
        classiferName = Configuration.classifier;
        trainingSet = new ArrayList<>();

        try {
            // connect training database
            this.trainingSetDB.connectDB(Configuration.trainingSetDBUrl, Configuration.trainingSetDBUser,
                                          Configuration.trainingSetDBPasswd, Configuration.trainingSetDBName);

        } catch (Exception e){
            e.printStackTrace();
        }

        logger.info("trainingSetDB connected. Going to loadBasicTrainingSet");
        System.out.println("trainingSetDB connected. Going to loadBasicTrainingSet");
        // load training set to this.trainingSet
        loadBasicTrainingSet();

        try {
            //close database connection
            this.trainingSetDB.closeDBConn();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("trainingSetDB connection closed");

        switch(classiferName){
            case "knn":
                this.classifer = new KNN();
                //case "decisionTree":
        }
        logger.info("Classifier: " + this.classiferName);
        System.out.println("Classifier: " + this.classiferName);
    }

    private boolean loadBasicTrainingSet(){
        String selectSQL = "SELECT * FROM `pages_table`";
        ResultSet selectRs;
        Statement selectStatement;
        try {
            selectRs = trainingSetDB.query(selectSQL);
            selectStatement = selectRs.getStatement();

            Map<String, String> dataMap;

            //set cursor to beginning
            selectRs.beforeFirst();

            while(selectRs.next()) {
                dataMap = new HashMap<String, String>();
                dataMap.put("page_id", selectRs.getString("page_id"));
                dataMap.put("page_url", selectRs.getString("page_url"));
                dataMap.put("domain_name", selectRs.getString("domain_name"));
                dataMap.put("title", selectRs.getString("title"));
                dataMap.put("keywords", selectRs.getString("keywords"));
                dataMap.put("description", selectRs.getString("description"));
                /*FIXME: Note that we don't include the `text` part now because it's so large that
                 * it may slow down the program */
                //dataMap.put("text", selectRs.getString("text"));
                dataMap.put("tag", selectRs.getString("tag"));
                //dataMap.put("PR_score", selectRs.getString("PR_score"));
                //dataMap.put("ad_NR", selectRs.getString("ad_NR"));

                trainingSet.add(dataMap);

            }
            //close selectRs
            selectStatement.close();
            selectRs.close();
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    //TODO: make it concurrent
    public void handle(){

        logger.info("Building training Model");
        System.out.println("Building training Model");
        classifer.buildTrainingModel(trainingSet);
        logger.info("Done built training Model");
        System.out.println("Done built training Model");

        //load data set and classifier it
        //connect database
        try {
            dataSetDB.connectDB(Configuration.sourceDBUrl, Configuration.sourceDBUser,
                    Configuration.sourceDBPasswd, Configuration.sourceDBName);
            resultSetDB.connectDB(Configuration.targetDBUrl, Configuration.targetDBUser,
                                   Configuration.targetDBPasswd, Configuration.targetDBName);
        } catch (Exception e) {
            e.printStackTrace();
        }


        //TODO: should ensure that this table exists
        String selectSQL = "SELECT * FROM `pages_table`";
        List<Map<String, String>> dataPart;
        ResultSet rs;
        Statement rsStatement;
        //TODO: this temporary setting would be removed
        int roundNum = 100;  // classify 100 tuple at a time
        try {
            rs = dataSetDB.query(selectSQL);
            rsStatement = rs.getStatement();
            rs.beforeFirst();
            while(rs.next()) {
                dataPart = new ArrayList<>();
                Map<String, String> record;
                for (int i = 0; i < roundNum; i++) {
                    if (rs.next()) {
                        record = new HashMap<>();
                        record.put("page_id", rs.getString("page_id"));
                        record.put("page_url", rs.getString("page_url"));
                        record.put("domain_name", rs.getString("domain_name"));
                        record.put("title", rs.getString("title"));
                        record.put("keywords", rs.getString("keywords"));
                        record.put("description", rs.getString("description"));
                        //record.put("text", rs.getString("text"));

                        dataPart.add(record);
                    } else {
                        break;
                    }
                }

                //start to classify part of the data set
                List<Map<String, String>> partResult = classifer.classify(dataPart);

                //store it back to result database
                String storeSQL = "INSERT INTO `classify_result`(page_id, page_url," +
                        " domain_name, title, keywords, description, tag)" +
                        " VALUES(?, ?, ?, ?, ?, ?, ?) ";
                for(Map<String, String> part : partResult) {
                    //TODO: the `text` field maybe so larget that it slow down the program
                    //TODO: we can use ansej to remove html tags
                    resultSetDB.insert(storeSQL, part.get("page_id"),
                                                 part.get("page_url"),
                                                 part.get("domain_name"),
                                                 part.get("title"),
                                                 part.get("keywords"),
                                                 part.get("description"),
                                                 //part.get("text"),
                                                 part.get("tag"));

                }
            }

            rsStatement.close();
            rs.close();

            //close database connection
            trainingSetDB.closeDBConn();
            resultSetDB.closeDBConn();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}













