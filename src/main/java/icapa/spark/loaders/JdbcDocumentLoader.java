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
            //.format("jdbc")
            .format("com.microsoft.sqlserver.jdbc.spark")
            .option("url", "jdbc:sqlserver://localhost;databaseName=playground;integratedSecurity=true;")
            .option("query", "SELECT note_id AS documentId, note AS text FROM playground.dbo.note")
            //.option("dbtable", "note")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load();
        dataset.collect();
        //Dataset<Row> dataset = sparkSession.sql("SELECT...");
        Dataset<Document> documents = dataset.as(Encoders.bean(Document.class));
        return documents;
    }
}
