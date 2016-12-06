package cn.lasagna.www.classifier;

import java.util.HashMap;

public class CompareRecord {
	private int page_id;
	private String tag;
	private HashMap<String,Double> keywordMap;
	private HashMap<String,Double> titleMap;
	private HashMap<String,Double> descriptionMap;
	
	public CompareRecord(){
		this.page_id = 0;
		this.tag = null;
		this.keywordMap = new HashMap<String,Double>();
		this.titleMap = new HashMap<String,Double>();
		this.descriptionMap = new HashMap<String,Double>();
	}
	
	public void setPage_id(int id){
		this.page_id = id;
	}
	
	public void setTag(String tag){
		this.tag = tag;
	}
	
	public void setKeywordMap(HashMap<String,Double> keyword){
		this.keywordMap = keyword;
	}
	
	public void setTitleMap(HashMap<String,Double> title){
		this.titleMap = title;
	}
	
	public void setDescriptonMap(HashMap<String,Double> description){
		this.descriptionMap = description;
	}
	
	public int getPage_id(){
		return page_id;
	}
	
	public String getTag(){
		return tag;
	}
	
	public HashMap<String,Double> getKeyword(){
		return keywordMap;
	}
	
	public HashMap<String,Double> getTitle(){
		return titleMap;
	}
	
	public HashMap<String,Double> getDescripton(){
		return descriptionMap;
	}

}
