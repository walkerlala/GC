package cn.lasagna.www.classifier;

public class TfIdf {
	private String word_id;
	private int page_id;
	private String word_value;
	private String word_tag;
	private double word_tf;
	private double word_idf;
	private double word_tf_idf;
	
	public TfIdf(){
		this.word_id = null;
		this.page_id = 0;
		this.word_value = null;
		this.word_tf = 0.0;
		this.word_idf = 0.0;
		this.word_tf_idf = 0.0;
	}
	
	public void setWord_id(String id){
		this.word_id = id;
	}
	
	public void setPage_id(int id){
		this.page_id = id;
	}
	
	public void setWord_value(String value){
		this.word_value = value;
	}
	
	public void setWord_tf(double tf){
		this.word_tf = tf;
	}
	
	public void setWord_idf(double idf){
		this.word_idf = idf;
	}
	
	public void setWord_tf_idf(double tf_idf){
		this.word_tf_idf = tf_idf;
	}
	
	public String getWord_id(){
		return this.word_id;
	}
	
	public int getPage_id(){
		return this.page_id;
	}
	
	public String getWord_value(){
		return this.word_value;
	}
	
	public double  getWord_tf(){
		return this.word_tf;
	}
	
	public double  getWord_idf(){
		return this.word_idf;
	}
	
	public double  getWord_tf_idf(){
		return this.word_tf_idf;
	}
	
	public void putAll(TfIdf ti){
		this.setWord_id(ti.getWord_id());
		this.setPage_id(ti.getPage_id());
		this.setWord_value(ti.getWord_value());
		this.setWord_tf(ti.getWord_tf());
		this.setWord_idf(ti.getWord_idf());
		this.setWord_tf_idf(ti.getWord_tf_idf());
	}
}
