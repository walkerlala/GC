package cn.lasagna.www.classifier;

import java.util.*;

public class TfIdfPool implements Iterable<TfIdf>{
	private List<TfIdf>dataInternal;
	
	public TfIdfPool(){
		dataInternal = new LinkedList<>();
	}
	
	public void add(TfIdf ti){
		dataInternal.add(ti);
	}
	
	public void addAll(TfIdfPool tip){
		for(TfIdf ti : tip)
			dataInternal.add(ti);
	}

	public TfIdf pop(){
		try {
            TfIdf tfidf = dataInternal.get(this.dataInternal.size() - 1);
            dataInternal.remove(this.dataInternal.size() - 1);
            return tfidf;
        }catch (Exception e){
            throw e;
        }
	}
	
	public TfIdf get(int index){
		return this.dataInternal.get(index);
	}
	
	public int size(){
		return this.dataInternal.size();
	}
	
	public void sort(Comparator<TfIdf> comparator){
		try {
            Collections.sort(this.dataInternal, comparator);
        }catch (Exception e){
            throw e;
        }
	}
	
	@Override
	public Iterator<TfIdf> iterator() {
		// TODO Auto-generated method stub
		return dataInternal.iterator();
	}
}
