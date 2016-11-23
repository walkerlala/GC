package cn.lasagna.www.classifier;

import java.util.*;

/**
 * Created by walkerlala on 16-11-15.
 */
public class RecordPool implements Iterable<Record> {


    private List<Record> dataInternal;
    //private int dataInternalIndex;

    public RecordPool(){
        dataInternal = new LinkedList<>();
        //dataInternalIndex = 0;
    }

    public void add(Record record){
        dataInternal.add(record);
    }

    public void addAll(Collection clt){
        dataInternal.addAll(clt);
    }

    public Record pop(){
        try {
            Record record = dataInternal.get(this.dataInternal.size() - 1);
            dataInternal.remove(this.dataInternal.size() - 1);
            return record;
        }catch (Exception e){
            throw e;
        }
    }

    public Record get(int index){
        return this.dataInternal.get(index);
    }

    public void clear(){
        this.dataInternal.clear();
    }

    public int size() { return this.dataInternal.size();}

    public Iterator<Record> iterator(){
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

    public void sort(Comparator<Record> comparator){
        try {
            Collections.sort(this.dataInternal, comparator);
        }catch (Exception e){
            throw e;
        }
    }
}
