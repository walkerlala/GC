package cn.lasagna.www.util;

import org.apache.log4j.Logger;
import org.nlpcn.commons.lang.util.logging.Log;

/**
 * Created by walkerlala on 16-11-2.
 */
public class MyLogger {

    private Logger lg;
    public static int STDOUT = 1;
    public static int STDERR = 2;

    public MyLogger(Class clz){
        lg = Logger.getLogger(clz);
    }

    public MyLogger(String name){
        lg = Logger.getLogger(name);
    }

    public void info(String msg){
        lg.info(msg);
    }

    /*
     * @msg
     * @direction: 1 -- print to stdout, else to stderr
     */
    public void info(String msg, int direction){
        if(direction==1){
           System.out.println(msg);
        }else{
            System.err.println(msg);
        }
        lg.info(msg);
    }

}
