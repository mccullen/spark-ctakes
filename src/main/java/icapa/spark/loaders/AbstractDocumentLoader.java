package icapa.spark.loaders;

import icapa.spark.models.Document;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractDocumentLoader {
    private SparkSession _sparkSession;
    private JavaSparkContext _javaSparkContext;

    public void init(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        _sparkSession = sparkSession;
        _javaSparkContext = javaSparkContext;
    }

    public abstract Dataset<Document> getDocuments();

    public SparkSession getSparkSession() {
        return _sparkSession;
    }

    public JavaSparkContext getJavaSparkContext() {
        return _javaSparkContext;
    }
}
