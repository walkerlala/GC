package cn.lasagna.www.classifier.Bayes;

import cn.lasagna.www.classifier.Record;
import cn.lasagna.www.classifier.RecordPool;
import cn.lasagna.www.classifier.ClassifierInterface;
import cn.lasagna.www.classifier.Preprocessor;
import cn.lasagna.www.util.Configuration;


import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Bayes implements ClassifierInterface{
	private Preprocessor prepro = new Preprocessor();
	private static ArrayList<HashMap<String,Double>> map = new ArrayList<HashMap<String,Double>>();//训练集12个类
	private int numOfTag = Configuration.numOfTag;
	
	private String tag;//每一个网页的标签
	private double[] pc = new double[numOfTag];//先验概率
	private double[] pxc = new double[numOfTag];//一个单词条件概率
	private double[] Pxc = new double[numOfTag];//一条记录条件概率
	private double[] p = new double[numOfTag];//bayes rule
	
	private String tag0 = Configuration.tag0;
	private String tag1 = Configuration.tag1;
	private String tag2 = Configuration.tag2;
	private String tag3 = Configuration.tag3;
	private String tag4 = Configuration.tag4;
	private String tag5 = Configuration.tag5;
	private String tag6 = Configuration.tag6;
	private String tag7 = Configuration.tag7;
	private String tag8 = Configuration.tag8;
	private String tag9 = Configuration.tag9;
	private String tag10 = Configuration.tag10;
	private String tag11 = Configuration.tag11;

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
		 map = prepro.getWordFreBayes(trainingSet);
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
		 
		 String word = generateWords(ket_des_tit);
		 
		 return word;
	 }
	 
	 /* return word list seperate by . */
	 public static String generateWords(String str){
	     List<Term> termList = NlpAnalysis.parse(str).getTerms();
	         
	     //remove duplicate
	     Set<Term> termSet = new HashSet<>();
	     termSet.addAll(termList);
	     termList.clear();
	     termList.addAll(termSet);

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
			 switch(newRecord.getTag()){
				case "新闻":
					num[0] += 1;
					break;
				case "小说":
					num[1] += 1;
					break;
				case "购物":
					num[2] += 1;
					break;
				case "医疗":
					num[3] += 1;
					break;
				case "视频":
					num[4] += 1;
					break;
				case "社交":
					num[5] += 1;
					break;
				case "游戏":
					num[6] += 1;
					break;
				case "旅游":
					num[7] += 1;
					break;
				case "服务":
					num[8] += 1;
					break;
				case "网络课程":
					num[9] += 1;
					break;
				case "音乐":
					num[10] += 1;
					break;
				case "软件下载":
					num[11] += 1;
					break;
				default:
					break;
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
				 pc[i] = num[i]/total_num;
		 } catch(ArithmeticException e){
			 System.out.println("分母为０！");
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
		
		switch(max){
		case 0:
			tag = tag0;
			break;
		case 1:
			tag = tag1;
			break;
		case 2:
			tag = tag2;
			break;
		case 3:
			tag = tag3;
			break;
		case 4:
			tag = tag4;
			break;
		case 5:
			tag = tag5;
			break;
		case 6:
			tag = tag6;
			break;
		case 7:
			tag = tag7;
			break;
		case 8:
			tag = tag8;
			break;
		case 9:
			tag = tag9;
			break;
		case 10:
			tag = tag10;
			break;
		case 11:
			tag = tag11;
			break;
		default:
			tag = "";
			break;	
		}
		
		return tag;
	 }
}
