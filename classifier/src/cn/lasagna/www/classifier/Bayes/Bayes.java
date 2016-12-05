package cn.lasagna.www.classifier.Bayes;

import cn.lasagna.www.classifier.Record;
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.Preprocessor;
import cn.lasagna.www.util.Configuration;


import cn.lasagna.www.util.MyLogger;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Bayes implements ClassifierInterface{
	MyLogger logger = new MyLogger(this.getClass());
	private static ArrayList<HashMap<String,Double>> map = new ArrayList<HashMap<String,Double>>();//训练集12个类
	private int numOfTag = Configuration.numOfTag;
	
	private String tag;//每一个网页的标签
	private double[] pc = new double[numOfTag];//先验概率
	private double[] pxc = new double[numOfTag];//一个单词条件概率
	private double[] Pxc = new double[numOfTag];//一条记录条件概率
	private double[] p = new double[numOfTag];//bayes rule

	private String[] tags = Configuration.tags;

	public Bayes(){
		for(int i=0;i<numOfTag;i++){
			pc[i] = 0;
			pxc[i] = 0;
			Pxc[i] = 0;
			p[i] = 0;
		}
	} 
	//得到12个哈希表
	 public boolean buildTrainingModel(RecordPool trainingSet) {
		 calculateClassPro(trainingSet);
		 map = getWordFreBayes(trainingSet);
		// numOfTag = map.size();
		 //System.out.println("****************"+map.size());
		 
		 return true;
	 }
	 
	 //把原始记录传进来，返回已加标签的记录
	 public RecordPool classify(RecordPool dataSet) {
		 RecordPool classifiedSet = new RecordPool();
	     Record newRecord;
	     for (Record tuple : dataSet) {
	          newRecord = new Record();
	          newRecord.putAll(tuple);
	          String line = testSample(newRecord);//分词
	          if(line.length()<=0){
	        	  newRecord.setTag(""); 
	        	  //System.out.println("*************** "+newRecord.getPage_id()+": "+line);
	          }
	          //System.out.println("*************** "+newRecord.getPage_id()+": "+line);
	          else{
	        	  //System.out.println("*************** "+newRecord.getPage_id()+": "+line);
		          String[] words = line.split(",");
		          calculatePro(words);//计算p
		         // System.out.println("*************** "+newRecord.getPage_id()+": ");
		          //for(int j=0;j<numOfTag;j++)
		        	 // System.out.println(j+": "+p[j]+" | ");
		          tag = getTag(p);
		          newRecord.setTag(tag);  // this should change `dataSet`
	          }
	          classifiedSet.add(newRecord);
	     }
	     return classifiedSet;     
	 }
	 
	 //得到分好词的记录
	 public String testSample(Record newRecord){
		 String keyword = newRecord.getKeywords();
		 String description = newRecord.getDescription();
		 String title = newRecord.getTitle();
		 String ket_des_tit = keyword + description+title;
		 
		 String word = Preprocessor.generateWords(ket_des_tit);
		 
		 return word;
	 }
	 
	 //计算一条记录在12个类别的概率
	 public void calculatePro(String[] words){
		 for(int j=0;j<numOfTag;j++){//每条记录重置概率
			 p[j] = 0.0;
			 Pxc[j] = 0.0;
		 }
		 for(int i=0;i<words.length;i++){
			//System.out.println(word[i]);
			 calculateConPro(words[i]);
			 for(int j=0;j<numOfTag;j++){
				 //System.out.println(pxc[j]);
				 Pxc[j] += Math.log(pxc[j]);
			 }
		 }
		 
		 for(int i=0;i<numOfTag;i++){
			 p[i] = Pxc[i];//没算先验概率的结果
			 //p[i] = Math.log(pc[i]) + Pxc[i];//最终概率
		 }
		 
	 }
	 
	 //计算先验概率,只算一遍
	 public void calculateClassPro(RecordPool Set){
		 int[] num = new int[numOfTag];
		 for(int i=0;i<numOfTag;i++)
			 num[i] = 0;
		 
		 //计算训练集里每个类的数量
		 Record newRecord;
		 for (Record tuple : Set){
			 newRecord = new Record();
	         newRecord.putAll(tuple);
			 String tagName = newRecord.getTag();
			 for(int i = 0; i<numOfTag; i++){
				 if(tagName.equals(this.tags[i])){
					 num[i]++;
				 }
			 }
		 }
		 
		 int total_num = 0;
		 for(int i=0;i<numOfTag;i++){
			 //System.out.println(i+": "+num[i]);
			 total_num += num[i];
		 }
		 //System.out.println(total_num);
		 try{
			 for(int i=0;i<numOfTag;i++)
				 pc[i] = (double)num[i]/total_num;
		 } catch(ArithmeticException e){
			 logger.info("分母为0!", MyLogger.STDOUT);
			 e.printStackTrace();
		 }

	 }
	 
	 //计算条件概率
	 public void calculateConPro(String word){
		 for(int i=0;i<numOfTag;i++){
			pxc[i] = 1.0;
		 }
		 
		int[] con = new int[numOfTag];//是否在类别里面，1在 0不在
		double[] tf = new double[numOfTag];//词频率
		for(int i=0;i<numOfTag;i++){
			tf[i] = 0;
			con[i] = 0;
		}
		 //词频率算pxc
		for(int i=0;i<numOfTag;i++){
			if(map.get(i).containsKey(word)){
				tf[i] = map.get(i).get(word)*1000;//放大1000倍，防止溢出
				con[i] = 1;
			}
		}
		
		//平滑处理
		int CON = 0;
		for(int i=0;i<numOfTag;i++){
			CON +=con[i];
		}
		
		if(CON == 0)//都不在关键词里面
		{
			//System.out.println("都不在关键词里");
		}
		else{
			
			//找出最小的概率
			int min = 0;
			for(int j = 0; j<numOfTag; j++){
				if(tf[j] != 0){
					min = j;
					break;
				}
			}
			for( int i = min+1;  i < numOfTag; i ++){
				if( tf[min] > tf[i] && tf[i]!=0 )
					min = i;	
			}
			double smooth = tf[min]/2;//最小概率的1/2
			
			for(int i=0;i<numOfTag;i++){//遍历12个类，不在此类关键词里面，要平滑概率
				if(con[i] == 0){
					pxc[i]=smooth;
				}
				else{	
					pxc[i]=tf[i];
				}
			}
		}

	 }
	 
	 //返回每条record的标签，如“新闻”
	 public String getTag(double[] array){
		int max = 0;//概率最大的下标
		for( int i = 1;  i < array.length; i ++){
			if( array[max] < array[i] )
				max = i;	
		}
		 tag = this.tags[max];

		return tag;
	 }

	 /* getWordFreBayes -- calculate word frequency for Bayes Classifier
  	 *  @pool: record pool
  	 *  return  --  every tag is associated with a map, which map a word to its frequency
  	 *              in the document of that word
  	 */
  	private ArrayList<HashMap<String,Double>> getWordFreBayes(RecordPool pool){
  		ArrayList<HashMap<String,Double>> map = new ArrayList<HashMap<String,Double>>();
  		int[] numOfEveryTag  = new int[numOfTag];
  		for(int i=0; i <numOfTag; i++){
  			if(!tags[i].equals("")){
  				HashMap<String,Double> tmp = new HashMap<String,Double>();
  				map.add(tmp);
  			}
  		}
  		String[] content;
  		//calculate every tag's word frequency
  		for(Record record : pool){
  			String rc_tag = record.getTag();
  			String title = record.getTitle();
  			String keywords = record.getKeywords();
       	    String description = record.getDescription();
       	    String document = title + keywords+ description;
       	    for(int i=0; i<map.size(); i++){
       	    	if(rc_tag.equals(tags[i])){
       	    		if(document.length()>=2){
               		    content = document.split(",");
               		    numOfEveryTag[i] += content.length;
               		    for(String word : content){
               		    	if(map.get(i).containsKey(word))  map.get(i).put(word, map.get(i).get(word)+1.0);
               		    	else  map.get(i).put(word, 1.0);
               		    }
               	    }
       	    		break;
       	    	}
       	    }
  		}
  		for(int i=0; i<map.size(); i++)
  			for(String key : map.get(i).keySet())
                map.get(i).put(key, map.get(i).get(key)/numOfEveryTag[i]);

  		return map;
  	}
}
