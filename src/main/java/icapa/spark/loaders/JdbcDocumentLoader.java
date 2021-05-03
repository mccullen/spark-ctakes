package icapa.spark.loaders;

import icapa.spark.models.Document;
import org.apache.spark.sql.*;

public class JdbcDocumentLoader extends AbstractDocumentLoader {
    private String _url;
    private String _query;
    private String _driver;
    private String _documentIdColumn;
    private String _textColumn;

    public JdbcDocumentLoader(String url, String driver, String query, String documentIdColumn, String textColumn) {
        _url = url;
        _query = query;
        _driver = driver;
        _documentIdColumn = documentIdColumn;
        _textColumn = textColumn;
    }

    @Override
    public Dataset<Document> getDocuments() {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> dataset = sparkSession.read()
            .format("jdbc")
            .option("url", _url)
            .option("query", _query)
            .option("driver", _driver)
            .load();
        //Dataset<Row> dataset = sparkSession.sql("SELECT...");
        Dataset<Document> documents = dataset.as(Encoders.bean(Document.class));
        return documents;
    }
}
