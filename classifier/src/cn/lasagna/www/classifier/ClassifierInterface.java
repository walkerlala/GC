package cn.lasagna.www.classifier;

import java.util.List;
import java.util.Map;

/**
 * Created by walkerlala on 16-10-24.
 */

/* all classifier should implement this interface */
public interface ClassifierInterface {
    public boolean buildTrainingModel(RecordPool trainingSet);
    public RecordPool classify(RecordPool dataSet);
}
