package icapa.spark.functions;

import icapa.spark.models.Document;
import org.apache.spark.api.java.function.ForeachPartitionFunction;

import java.util.Iterator;

public class PipelineForEachPartition implements ForeachPartitionFunction<Document> {
    @Override
    public void call(Iterator<Document> iterator) throws Exception {

    }
}
