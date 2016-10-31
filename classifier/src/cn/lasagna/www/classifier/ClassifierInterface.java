package cn.lasagna.www.classifier;

import java.util.List;
import java.util.Map;

/**
 * Created by walkerlala on 16-10-24.
 */

/* all classifier should implement this interface */
public interface ClassifierInterface {
    public boolean buildTrainingModel(List<Map<String, String>> trainingSet);
    public List<Map<String, String>> classify(List<Map<String, String>> dataSet);
}
