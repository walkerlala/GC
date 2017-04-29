package cn.lasagna.www.driver;

/**
 * Created by walkerlala on 16-10-23.
 */
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.lasagna.www.classifier.Preprocessor;

public class ClassifyDriver {
    public static void main(String[] args){
        String launchTime = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
        System.out.println("Driver start running at " + launchTime);

        Preprocessor pre = new Preprocessor();
        pre.preprocess();
        
        //ClassifyHandler classifyHandler = new ClassifyHandler();
        //classifyHandler.handle();

        String stopTime = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
        System.out.println("Driver stop at " + stopTime);
    }
}
