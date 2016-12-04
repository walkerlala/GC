package cn.lasagna.www.util;

/**
 * Created by walkerlala on 16-10-23.
 */

import cn.lasagna.www.classifier.knn.KNN;
//import org.jcp.xml.dsig.internal.dom.DOMUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.DoubleSummaryStatistics;
import java.util.Properties;

public class Configuration {

    //forbit instantiating
    private Configuration() {

    }

    public static final String sourceDBUrl;
    public static final String sourceDBUser;
    public static final String sourceDBPasswd;
    public static final String sourceDBName;

    public static final String trainingSetDBUrl;
    public static final String trainingSetDBUser;
    public static final String trainingSetDBPasswd;
    public static final String trainingSetDBName;
    
    public static final String tfidfDBUrl;
    public static final String tfidfDBUser;
    public static final String tfidfDBPasswd;
    public static final String tfidfDBName;

    public static final String targetDBUrl;
    public static final String targetDBUser;
    public static final String targetDBPasswd;
    public static final String targetDBName;

    public static final String classifier;
    public static final int K;
    public static final double KNNKeywordsWeight;
    public static final double KNNTitleWeight;
    public static final double KNNDescriptionWeght;
    
    public static final int numOfTag;
    public static final String tag0;
    public static final String tag1;
    public static final String tag2;
    public static final String tag3;
    public static final String tag4;
    public static final String tag5;
    public static final String tag6;
    public static final String tag7;
    public static final String tag8;
    public static final String tag9;
    public static final String tag10;
    public static final String tag11;

    //instantiate
    static {
        Properties prop = new Properties();
        try {
            InputStream inputStream = new BufferedInputStream(
                    new FileInputStream("conf/Configuration.properties"));
            prop.load(inputStream);
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        sourceDBUrl = prop.getProperty("sourceDBUrl");
        sourceDBUser = prop.getProperty("sourceDBUser");
        sourceDBPasswd = prop.getProperty("sourceDBPasswd");
        sourceDBName = prop.getProperty("sourceDBName");

        trainingSetDBUrl = prop.getProperty("trainingDBUrl");
        trainingSetDBUser = prop.getProperty("trainingDBUser");
        trainingSetDBPasswd = prop.getProperty("trainingDBPasswd");
        trainingSetDBName = prop.getProperty("trainingDBName");
        
        tfidfDBUrl = prop.getProperty("tfidfDBUrl");
        tfidfDBUser = prop.getProperty("tfidfDBUser");
        tfidfDBPasswd = prop.getProperty("tfidfDBPasswd");
        tfidfDBName = prop.getProperty("tfidfDBName");

        targetDBUrl = prop.getProperty("targetDBUrl");
        targetDBUser = prop.getProperty("targetDBUser");
        targetDBPasswd = prop.getProperty("targetDBPasswd");
        targetDBName = prop.getProperty("targetDBName");

        classifier = prop.getProperty("classifier");
        K = Integer.valueOf(prop.getProperty("K"));
        KNNKeywordsWeight = Double.valueOf(prop.getProperty("KNNKeywordsWeight"));
        KNNTitleWeight = Double.valueOf(prop.getProperty("KNNTitleWeight"));
        KNNDescriptionWeght = Double.valueOf(prop.getProperty("KNNDescriptionWeght"));
        numOfTag = Integer.valueOf(prop.getProperty("numOfTag"));
        
        tag0 = prop.getProperty("tag0");
        tag1 = prop.getProperty("tag1");
        tag2 = prop.getProperty("tag2");
        tag3 = prop.getProperty("tag3");
        tag4 = prop.getProperty("tag4");
        tag5 = prop.getProperty("tag5");
        tag6 = prop.getProperty("tag6");
        tag7 = prop.getProperty("tag7");
        tag8 = prop.getProperty("tag8");
        tag9 = prop.getProperty("tag9");
        tag10 = prop.getProperty("tag10");
        tag11 = prop.getProperty("tag11");
    }

}
