package cn.lasagna.www.classifier;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CompareRecordPool implements Iterable<CompareRecord>{
	 private List<CompareRecord> dataInternal;
	    //private int dataInternalIndex;

	    public CompareRecordPool(){
	        dataInternal = new LinkedList<>();
	        //dataInternalIndex = 0;
	    }

	    public void add(CompareRecord record){
	        dataInternal.add(record);
	    }

	    public void addAll(Collection clt){
	        dataInternal.addAll(clt);
	    }

	    public CompareRecord pop(){
	        try {
	            CompareRecord record = dataInternal.get(this.dataInternal.size() - 1);
	            dataInternal.remove(this.dataInternal.size() - 1);
	            return record;
	        }catch (Exception e){
	            throw e;
	        }
	    }

	    public CompareRecord get(int index){
	        return this.dataInternal.get(index);
	    }

	    public void clear(){
	        this.dataInternal.clear();
	    }

	    public int size() { return this.dataInternal.size();}

	    public Iterator<CompareRecord> iterator(){
	        return dataInternal.iterator();
	        /*
	        return new Iterator<Record>() {
	            @Override
	            public boolean hasNext() {
	                return dataInternalIndex < dataInternal.size();
	            }

	            @Override
	            public Record next() {
	                return dataInternal.get(dataInternalIndex++);
	            }
	        };
	        */
	    }

	    public void sort(Comparator<CompareRecord> comparator){
	        try {
	            Collections.sort(this.dataInternal, comparator);
	        }catch (Exception e){
	            throw e;
	        }
	    }
}
