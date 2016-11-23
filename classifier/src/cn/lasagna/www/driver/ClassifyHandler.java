package cn.lasagna.www.driver;

/**
 * Created by walkerlala on 16-10-24.
 */
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.Record;
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.classifier.knn.KNN;
import cn.lasagna.www.util.Configuration;
import cn.lasagna.www.util.DBUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import cn.lasagna.www.util.MyLogger;
import org.apache.log4j.Logger;

/* TODO: should rebuild ambiguity.dic
 * TODO: should use similarity-dic
 * TODO: should use importance-dic
 */
public class ClassifyHandler {
    //Without any access qualifier(public, private, protected)desclared, logger has "package visibility"(default visibility in Java)
    //In Java there are public, protected, package (default), and private visibilities; ordered from most visible to least.
    //Logger logger = Logger.getLogger(this.getClass());
    MyLogger logger = new MyLogger(this.getClass());

    private String classiferName;
    private ClassifierInterface classifer;
    private RecordPool trainingSet;

    private DBUtil trainingSetDB = new DBUtil();
    private DBUtil dataSetDB = new DBUtil();
    private DBUtil resultSetDB = new DBUtil();

    public ClassifyHandler(){
        logger.info("ClassifierHandler initiating", MyLogger.STDOUT);
        classiferName = Configuration.classifier;
        trainingSet = new RecordPool();

        try {
            // connect training database
            this.trainingSetDB.connectDB(Configuration.trainingSetDBUrl, Configuration.trainingSetDBUser,
                                          Configuration.trainingSetDBPasswd, Configuration.trainingSetDBName);

        } catch (Exception e){
            e.printStackTrace();
        }

        logger.info("trainingSetDB connected. Going to loadBasicTrainingSet", MyLogger.STDOUT);
        // load training set to this.trainingSet
        loadBasicTrainingSet();

        try {
            //close database connection
            this.trainingSetDB.closeDBConn();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("trainingSetDB connection closed", MyLogger.STDOUT);

        switch(classiferName){
            case "knn":
                this.classifer = new KNN();
                //case "decisionTree":
        }
        logger.info("Classifier: " + this.classiferName, MyLogger.STDOUT);
    }

    private boolean loadBasicTrainingSet(){
        String selectSQL = "SELECT * FROM `pages_table`";
        ResultSet selectRs;
        Statement selectStatement;
        try {
            selectRs = trainingSetDB.query(selectSQL);
            selectStatement = selectRs.getStatement();

            Record record;

            //set cursor to beginning
            selectRs.beforeFirst();

            while(selectRs.next()) {
                record = new Record();
                record.setPage_id(selectRs.getString("page_id"));
                record.setPage_url(selectRs.getString("page_url"));
                record.setDomain_name(selectRs.getString("domain_name"));
                record.setTitle(selectRs.getString("title"));
                record.setKeywords(selectRs.getString("keywords"));
                record.setDescription(selectRs.getString("description"));
                /*FIXME: Note that we don't include the `text` part now because it's so large that
                 * it may slow down the program */
                //record.setText(selectRs.getString("text"));
                record.setTag(selectRs.getString("tag"));
                //record.setPR_score(selectRs.getDouble("PR_score"));
                //record.setAd_NR(selectRs.getLong("ad_NR"));
                this.trainingSet.add(record);

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

        logger.info("Building training Model", MyLogger.STDOUT);
        classifer.buildTrainingModel(trainingSet);
        logger.info("Done built training Model", MyLogger.STDOUT);

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


        //make sure that these table exists and clear
        try {
            resultSetDB.modify("CREATE TABLE IF NOT EXISTS `pages_table` (" +
                    "`page_id` int(20) NOT NULL AUTO_INCREMENT," +
                    "`page_url` varchar(400) NOT NULL," +
                    "`domain_name` varchar(100) NOT NULL," +
                    //"`sublinks` text," +
                    "`title` varchar(255)," +
                    //"`nomal_content` text," +
                    //"`emphasized_content` text," +
                    "`keywords` varchar(255)," +
                    "`description` varchar(511)," +
                    "`text` longtext," +
                    "`PR_score` double default 0.0," +
                    "`ad_NR` int default 0," +
                    "`tag` varchar(20) default null," +
                    //"`classify_attr1` ..." +
                    "PRIMARY KEY (`page_id`)" +
                    ")ENGINE=InnoDB");
            resultSetDB.modify("truncate table `pages_table`");
        }catch (Exception e){
            e.printStackTrace();
        }
        String selectSQL = "SELECT * FROM `pages_table`";
        RecordPool dataPart;
        ResultSet rs;
        Statement rsStatement;
        //TODO: this temporary setting would be removed
        int roundNum = 100;  // classify 100 tuple at a time
        try {
            rs = dataSetDB.query(selectSQL);
            rsStatement = rs.getStatement();
            rs.beforeFirst();
            while(rs.next()) {
                dataPart = new RecordPool();
                Record record;
                for (int i = 0; i < roundNum; i++) {
                    if (rs.next()) {
                        record = new Record();
                        record.setPage_id(rs.getString("page_id"));
                        record.setPage_url(rs.getString("page_url"));
                        record.setDomain_name(rs.getString("domain_name"));
                        record.setTitle(rs.getString("title"));
                        record.setKeywords(rs.getString("keywords"));
                        record.setDescription(rs.getString("description"));
                        //record.setText(rs.getString("text"));
                        dataPart.add(record);
                    } else {
                        break;
                    }
                }

                preprocessData(dataPart);
                //start to classify part of the data set
                RecordPool partResult = classifer.classify(dataPart);

                //store it back to result database
                String storeSQL = "INSERT INTO `pages_table`(page_id, page_url," +
                        " domain_name, title, keywords, description, tag)" +
                        " VALUES(?, ?, ?, ?, ?, ?, ?) ";
                for(Record part : partResult) {
                    //TODO: the `text` field maybe so larget that it slow down the program
                    //TODO: we can use ansej to remove html tags
                    resultSetDB.insert(storeSQL, part.getPage_id(),
                                                 part.getPage_url(),
                                                 part.getDomain_name(),
                                                 part.getTitle(),
                                                 part.getKeywords(),
                                                 part.getDescription(),
                                                 //part.getText(),
                                                 part.getTag());

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

    /* seperate word, remove stop word... */
    boolean preprocessData(RecordPool dataPart) {
        for(Record record : dataPart){
            String kw = record.getKeywords();
            String desc = record.getDescription();
            String newKw = KNN.generateWords(kw);
            String newDesc = KNN.generateWords(desc);
            record.setKeywords(newKw);
            record.setDescription(newDesc);
        }
        return true;
    }

}




