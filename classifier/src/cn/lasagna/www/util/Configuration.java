package cn.lasagna.www.util;

/**
 * Created by walkerlala on 16-10-23.
 */

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    public static final String targetDBUrl;
    public static final String targetDBUser;
    public static final String targetDBPasswd;
    public static final String targetDBName;

    public static final String classifier;

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

        targetDBUrl = prop.getProperty("targetDBUrl");
        targetDBUser = prop.getProperty("targetDBUser");
        targetDBPasswd = prop.getProperty("targetDBPasswd");
        targetDBName = prop.getProperty("targetDBName");

        classifier = prop.getProperty("classifier");
    }

}
