package cn.lasagna.www.classifier.neural;

import cn.lasagna.www.classifier.Record;
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.CompareRecord;
import cn.lasagna.www.classifier.CompareRecordPool;
import cn.lasagna.www.classifier.Preprocessor;

import cn.lasagna.www.util.Configuration;
import cn.lasagna.www.util.DBUtil;
import cn.lasagna.www.util.MyLogger;

import java.util.Collections;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.lang.Math;

public class NeturalNetwork implements ClassifierInterface {

    public boolean buildTrainingModel(RecordPool trainingSet) {
        return true;
    }

    public RecordPool classify(RecordPool dataSet) {

        return new RecordPool();
    }
 //   private enum Mod {TRAIN, APPLY}
 //
 //   //����ڵ���
 //   private int input_num;
 //   private int hidden_num;
 //   private int output_num;
 //   //����ڵ���ֵ
 //   private double[] inputlayer;
 //   private double[] hiddenlayer;
 //   private double[] outputlayer;
 //   private double[] outputlayer_predict;
 //   //Ȩ��
 //   private double[][] intohidlayer_weight;
 //   private double[][] hidtooulayer_weight;
 //   //��ֵ
 //   private double[] hiddenlayer_threshold;
 //   private double[] outputlayer_threshold;
 //   //ѧϰϵ��
 //   private double sita;
 //
 //   private final int fea_keyword_num = 10;       //ÿ����¼�õ���keyword
 //   private final int fea_description_num = 10;    //ÿ����¼�õ���description
 //   private final int fea_title_num = 10;            //ÿ����¼�õ���title
 //   private final int use_num = 100;                        //ÿ�����õ��ļ�¼��
 //   private final int fea_num = fea_keyword_num * use_num + fea_description_num * use_num +
 //           fea_title_num * use_num;//ÿһ������������������
 //   private final int class_num = 13;//�����
 //   private HashMap<String, Integer> featurePool[];   //StringΪ��������intΪ��ţ���Ӧ����ڵ�����
 //   private CompareRecordPool[] NNSet = new CompareRecordPool[class_num];      //��0~11�ֱ��������ѵ����
 //
 //   private MyLogger logger;
 //
 //
 //   private void clear() {
 //       for (int i = 0; i < input_num) ; i++){
 //           inputlayer[i] = 0;
 //       }
 //
 //       for (int i = 0; i < hidden_num) ; i++){
 //           hiddenlayer[i] = 0;
 //       }
 //
 //       for (int i = 0; i < output_num) ; i++){
 //           outputlayer[i] = 0;
 //           outputlayer_predict[i] = 0;
 //       }
 //   }
 //
 //   NeturalNetwork() {
 //       MyLogger logger = new MyLogger(this.getClass());
 //       logger.info("Netural network start to initialize...", Mylogger.STDOUT);
 //
 //       try {
 //           // connect training database
 //           this.tfidfDB.connectDB(Configuration.tfidfDBUrl, Configuration.tfidfDBUser,
 //                   Configuration.tfidfDBPasswd, Configuration.tfidfDBName);
 //       } catch (Exception e) {
 //           e.printStackTrace();
 //       }
 //       init();
 //   }
 //
 //   private void init() {
 //       featurePool = new HashMap<String, int>();
 //       for (int i = 0; i < class_num; i++) {
 //           NNSet[i] = new CompareRecord();
 //       }
 //
 //       sita = 1;
 //       input_num = fea_num * class_num;
 //       hidden_num = int(Math.log(input_num) / Math.log(2));
 //       output_num = class_num;
 //
 //       inputlayer = new double[input_num];
 //       hiddenlayer = new double[hidden_num];
 //       outputlayer = new double[output_num];
 //       outputlayer_predict = new double[output_num];
 //       intohidlayer_weight = new double[input_num][hidden_num];
 //       hidtooulayer_weight = new double[hidden_num][output_num];
 //       hiddenlayer_threshold = new double[hidden_num];
 //       outputlayer_threshold = new double[output_num];
 //
 //       clear();
 //
 //       for (int i = 0; i < input_num) ; i++)
 //       for (int j = 0; j < hidden_num; j++)
 //           intohidlayer_weight[i][j] = Math.random();
 //
 //       for (int i = 0; i < hidden_num) ; i++)
 //       for (int j = 0; j < output_num; j++)
 //           hidtooulayer_weight[i][j] = Math.random();
 //
 //       for (int i = 0; i < hidden_num) ; i++)
 //       hiddenlayer_threshold[i] = Math.random();
 //
 //       for (int i = 0; i < output_num) ; i++)
 //       outputlayer_threshold[i] = Math.random();
 //
 //       logger.info("Netural network has been initialized.", Mylogger.STDOUT);
 //   }
 //
 //   NeturalNetwork(int sita) {
 //       NetrualNetwork();
 //
 //       this sita = sita;
 //
 //       logger.info("Netural network's sita has been initialized.", Mylogger.STDOUT);
 //   }
 //
 //   public boolean buildTrainingModel(RecordPool trainingSet) {
 //       CompareRecord comRecord;
 //       for (Record record : trainingSet) {
 //           comRecord = tfidfValue(record);
 //           if (convertToNumber(comRecord.getTag()) != -1) {
 //               NNSet[convertToNumber(comRecord.getTag())].add(comRecord);
 //           }
 //       }
 //
 //       //������tfidf��ѡ��������ÿ��ȡifidfֵ��ߵ���Ϊ������
 //       featureChoose();
 //
 //       for (int i = 0; i < class_num; i++) {
 //           for (CompareRecord comR : NNSet[i]) {
 //               layer_cal(comR, TRAIN)��
 //               weight_update();
 //           }
 //       }
 //
 //       return true;
 //   }
 //
 //   private void featureChoose() {
 //       //���ź����ѵ������ȡ������������������
 //       //��class_num������ȡ��use_num����¼��
 //       //�����е�fea_keyword_num\fea_title_num\fea_description_num����������featurePool��
 //       int count = 0;
 //       for (int i = 0; i < class_num; i++) {
 //           for (int j = 0; j < use_num; j++) {
 //               for (String f : getKTD(NNSet[i], (j + 1), fea_keyword_num, "keyword"))
 //                   featurePool.put(f, (count++));
 //               for (String f : getKTD(NNSet[i], (j + 1), fea_title_num, "title"))
 //                   featurePool.put(f, (count++));
 //               for (String f : getKTD(NNSet[i], (j + 1), fea_description_num, "description"))
 //                   featurePool.put(f, (count++));
 //           }
 //       }
 //   }
 //
 //   //���ڻ�ȡCompareRecordPool�е�no��CompareRecord�Ļ�keyword��title��description������m����ǰnum��ֵ
 //   private String[] getKTD(CompareRecordPool comRecordPool, int no, int num, String m) {
 //       if (no < 0) {
 //           logger.info("Training set(" + m + ") is too small.", MyLogger.STDERR);
 //           String[] wrong = new String[1];
 //           wrong[0] = ("error");
 //           return wrong;
 //       }
 //       for (CompareRecord comRecord : comRecordPool) {
 //           if ((no--) == 1) {
 //               HashMap<String, Double> useSet;
 //
 //               //����tfidf��HashMap����
 //               switch (m) {
 //                   case "keyword":
 //                       List<Map.Entry<String, Double>> l = new ArrayList<Map.Entry<String, Double>>(comRecord.getKeyword().entrySet());
 //                       useSet = comRecord.getKeyword();
 //                       break;
 //
 //                   case "title":
 //                       List<Map.Entry<String, Double>> l = new ArrayList<Map.Entry<String, Double>>(comRecord.getTitle().entrySet());
 //                       useSet = comRecord.getTitle();
 //                       break;
 //
 //                   case "description":
 //                       List<Map.Entry<String, Double>> l = new ArrayList<Map.Entry<String, Double>>(comRecord.getDescription().entrySet());
 //                       useSet = comRecord.getDescription();
 //                       break;
 //
 //                   default:
 //                       List<Map.Entry<String, Double>> l = new ArrayList<Map.Entry<String, Double>>(comRecord.getKeyword().entrySet());
 //                       useSet = comRecord.getKeyword();    //������������Ϊʹ��keyword
 //                       break;
 //               }
 //               //����tfidfֵ��������
 //               Collections.sort(l, new Comparator<Map.Entry<String, Double>>) {
 //                   @Override
 //                   public int compare (Entry < String, Double > o1, Entry < String, Double > o2){
 //                       return o1.get() >= o2.get() ? 1 : -1;
 //                   }
 //               })
 //               //��ǰnum��key��ϳ����鷵��
 //               String[] use = new String[num];
 //               int count = 0;
 //               for (Map.Entry u : useSet.entrySet()) {
 //                   use[count++] = new String(u.getKey());
 //                   if (count > num) break;
 //               }
 //               return use;
 //           }
 //       }
 //   }
 //
 //   private CompareRecord tfidfValue(Record record) {
 //       //��tfidf���ݿ��в��һ���¼��һ��tfidfֵ���ٸ���keyword��title��description����HashMap�󷵻رȽϼ�¼
 //       String keywords = record.getKeywords();
 //       String description = record.getDescription();
 //       String title = record.getTitle();
 //       CompareRecord comRecord = new CompareRecord();
 //
 //       HashMap<String, Double> keywordMap = new HashMap<String, Double>();
 //       HashMap<String, Double> titleMap = new HashMap<String, Double>();
 //       HashMap<String, Double> descriptionMap = new HashMap<String, Double>();
 //
 //       String pageID = record.getPage_id();
 //       String selectSQL = "SELECT * FROM  `tfidf_training` WHERE page_id = " + pageID;
 //       ResultSet rs;
 //       Statement rsStatement;
 //       // load tfidf value into each hash map
 //       try {
 //           rs = tfidfDB.query(selectSQL);
 //           rsStatement = rs.getStatement();
 //           rs.beforeFirst();
 //           while (rs.next()) {
 //               String word = rs.getString("word_value");
 //               double tfidf_value = rs.getDouble("tf_idf");
 //               if (keywords.contains(word))
 //                   keywordMap.put(word, tfidf_value);
 //               if (description.contains(word))
 //                   descriptionMap.put(word, tfidf_value);
 //               if (title.contains(word))
 //                   titleMap.put(word, tfidf_value);
 //           }//end while
 //           rsStatement.close();
 //           rs.close();
 //       } catch (Exception e) {
 //           // TODO Auto-generated catch block
 //           logger.info("Can't select record value from tfidfDB. ", MyLogger.STDOUT);
 //           e.printStackTrace();
 //       }
 //
 //
 //       //Calculate each vector length if the item exist, otherwise set the vector null
 //       if (record.getDescription() != "")
 //           descriptionMap.put("0", calculateVector(descriptionMap));
 //       else
 //           descriptionMap = null;
 //       if (record.getKeywords() != "")
 //           keywordMap.put("0", calculateVector(keywordMap));
 //       else
 //           keywordMap = null;
 //       if (record.getTitle() != "")
 //           titleMap.put("0", calculateVector(titleMap));
 //       else
 //           titleMap = null;
 //
 //       //set compare record
 //       comRecord.setDescriptonMap(descriptionMap);
 //       comRecord.setKeywordMap(keywordMap);
 //       comRecord.setTitleMap(titleMap);
 //       comRecord.setPage_id(pageID);
 //       comRecord.setTag(record.getTag());
 //
 //       return comRecord;
 //   }
 //
 //   private double calculateVector(HashMap<String, Double> map) {
 //       ArrayList<Double> value = new ArrayList<Double>(map.values());
 //       double sum = 0;
 //       for (int i = 0, length = value.size(); i < length; i++)
 //           sum = sum + value.get(i);
 //
 //       return Math.sqrt(sum);
 //   }
 //
 //   public RecordPool classify(RecordPool dataSet) {
 //       RecordPool classifiedSet = new RecordPool();
 //       Record dataRecord;
 //
 //       for (Record tuple : dataSet) {
 //           dataRecord = new Record();
 //           dataRecord.putAll(tuple);
 //           CompareRecord comRecord = tfidfValue(dataRecord);
 //           clear();
 //           String tag = layer_cal(comRecord, APPLY)��
 //           dataRecord.setTag(tag);
 //       }
 //
 //       return classifiedSet;
 //   }
 //
 //   //���������ֵ�������ظ������������������
 //   private String layer_cal(CompareRecord comSet, Mod m) {
 //       //��������㣬�����ѵ���������������outputlayer[]
 //       inputLayer_cal(comSet);
 //       if (m == TRAIN)
 //           outputLay_cal(comSet);
 //
 //       //��������ֵ����
 //       for (int i = 0; i < hidden_num; i++)
 //           for (int j = 0; j < input_num; j++)
 //               hiddenlayer[i] += intohidlayer_weight[j][i] * inputlayer[j];
 //
 //       for (int i = 0; i < hidden_num; i++) {
 //           hiddenlayer[i] -= hiddenlayer_threshold[i];
 //           hiddenlayer[i] = 1 / (1 + Math.exp(hiddenlayer[i]));
 //       }
 //
 //       //�������ֵ����
 //       for (int i = 0; i < output_num; i++)
 //           for (int j = 0; j < hidden_num; j++)
 //               outputlayer_predict[i] += hidtooulayer_weight[j][i] * hiddenlayer[j];
 //
 //       for (int i = 0; i < output_num; i++) {
 //           outputlayer_predict[i] -= outputlayer_threshold[i];
 //           outputlayer_predict[i] = 1 / (1 + Math.exp(outputlayer_predict[i]));
 //       }
 //
 //       //�ж�tag
 //       int possibility = 0;
 //       for (int i = 0; i < (class_num - 2); i++) {
 //           if (outputlayer_predict[i + 1] > outputlayer_predict[i]) {
 //               possibility = i + 1;
 //           }
 //       }
 //
 //       return convertToName(possibility);
 //   }
 //
 //   private void inputLayer_cal(CompareRecord comRecord) {
 //       HashMap<String, Double> keywordMap = comRecord.getKeyword();
 //       HashMap<String, Double> titleMap = comRecord.getTitle();
 //       HashMap<String, Double> descriptionMap = comRecord.getDescription();
 //       //TODO:����Ǵ�keyword����ȡ���������ոպó�����һ��title������֮��ȣ�Ҳ��������
 //       for (Map.Entry fp : featurePool.entrySet()) {
 //           for (Map.Entry km : keywordMap.entrySet()) {
 //               if (fp.getKey().equal(km.getKey())) {
 //                   inputlayer[fp.getValue()] += km.getValue();
 //               }
 //           }
 //           for (Map.Entry tm : titleMap.entrySet()) {
 //               if (fp.getKey().equal(tm.getKey())) {
 //                   inputlayer[fp.getValue()] += tm.getValue();
 //               }
 //           }
 //           for (Map.Entry dm : descriptionMap.entrySet()) {
 //               if (fp.getKey().equal(dm.getKey())) {
 //                   inputlayer[fp.getValue()] += dm.getValue();
 //               }
 //           }
 //       }
 //   }
 //
 //   private void outputLayout_cal(CompareRecord comRecord) {
 //       if (convertToNumber(comRecord.getTag()) != -1)
 //           outputlayer[convertToNumber(comRecord.getTag())] = 1;
 //   }
 //
 //   private boolean weight_update() {
 //       double g[ output_num] ={
 //           0
 //       } ;
 //       double e[ hidden_num] ={
 //           0
 //       } ;
 //
 //       for (int i = 0; i < output_num; i++)
 //           g[i] = outputlayer_predict[i] * (1 - outputlayer_predict[i]) * (outputlayer[i] - outputlayer_predict[i]);
 //
 //       for (int i = 0; i < hidden_num; i++) {
 //           double wg = 0;
 //           for (int j = 0; j < output_num; j++) {
 //               wg += hidtooulayer_num[i][j] * g[j];
 //           }
 //           e[i] = hiddenlayer[i] * (1 - hiddenlayer[i]) * wg;
 //       }
 //
 //       for (int i = 0; i < input_num; i++)
 //           for (int j = 0; j < hidden_num; j++)
 //               intohidlayer_weight[i][j] += sita * e[j] * inputlayer[i];
 //
 //       for (int i = 0; i < hidden_num; i++)
 //           for (int j = 0; j < output_num; i++)
 //               hidtooulayer_weight[i][j] += sita * g[j] * hiddenlayer[i];
 //
 //       for (int i = 0; i < hidden_num; i++)
 //           hiddenlayer_threshold[i] -= sita * e[i];
 //
 //       for (int i = 0; i < output_num; i++)
 //           outputlayer_threshold[i] -= sita * g[i];
 //
 //       return true;
 //   }
 //
 //   private int convertToNumber(String className) {
 //       switch (className) {
 //           case "����":
 //               return 0;
 //           case "�Ķ�":
 //               return 1;
 //           case "����":
 //               return 2;
 //           case "ҽ��":
 //               return 3;
 //           case "��Ƶ":
 //               return 4;
 //           case "�罻":
 //               return 5;
 //           case "��Ϸ":
 //               return 6;
 //           case "����":
 //               return 7;
 //           case "����":
 //               return 8;
 //           case "�������":
 //               return 9;
 //           case "����":
 //               return 10;
 //           case "�������":
 //               return 11;
 //           case "�����":
 //               return 12;
 //           default:
 //               mylogger.info("Unrecognized className occured: " + className, Mylogger.STDERR);
 //               return -1;
 //       }
 //   }
 //
 //   private String convertToName(int classNumber) {
 //       switch (classNumber) {
 //           case 0:
 //               return "����";
 //           case 1:
 //               return "�Ķ�";
 //           case 2:
 //               return "����";
 //           case 3:
 //               return "ҽ��";
 //           case 4:
 //               return "��Ƶ";
 //           case 5:
 //               return "�罻";
 //           case 6:
 //               return "��Ϸ";
 //           case 7:
 //               return "����";
 //           case 8:
 //               return "����";
 //           case 9:
 //               return "�������";
 //           case 10:
 //               return "����";
 //           case 11:
 //               return "�������";
 //           case 12:
 //               return "�����";
 //           default:
 //               mylogger.info("Unrecognized classNumber occured: " + classNumber, Mylogger.STDERR);
 //               return "�޷���";
 //       }
//   }

}