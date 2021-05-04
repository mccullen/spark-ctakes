package icapa.spark.loaders;

import icapa.spark.Util;
import icapa.spark.models.Document;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.List;

public class StressTestLoader extends AbstractLoader {
    private int _nDocuments;

    public StressTestLoader(String nDocuments) {
        _nDocuments = Integer.parseInt(nDocuments);
    }

    @Override
    public Dataset<Document> getDocuments() {
        List<Document> testDocs = Util.getTestDocuments(_nDocuments);
        JavaRDD<Document> testDocsRDD = getJavaSparkContext().parallelize(testDocs);
        Dataset<Row> dataset = getSparkSession().createDataFrame(testDocsRDD, Document.class);
        Dataset<Document> result = dataset.as(Encoders.bean(Document.class));
        return result;
    }
}
