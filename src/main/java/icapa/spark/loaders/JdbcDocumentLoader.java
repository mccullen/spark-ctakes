package icapa.spark.loaders;

import icapa.spark.models.Document;
import org.apache.spark.sql.*;

public class JdbcDocumentLoader extends AbstractDocumentLoader {
    @Override
    public Dataset<Document> getDocuments() {
        SparkSession sparkSession = getSparkSession();
        //sparkSession.read().format("jdbc").option("","").loa
        Dataset<Row> dataset = sparkSession.sql("SELECT...");
        Dataset<Document> documents = dataset.as(Encoders.bean(Document.class));
        return documents;
    }
}
