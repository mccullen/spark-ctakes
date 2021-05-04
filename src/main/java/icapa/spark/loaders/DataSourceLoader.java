package icapa.spark.loaders;

import icapa.spark.models.Document;
import org.apache.spark.sql.*;

public class DataSourceLoader extends AbstractLoader {
    public static final String FORMAT = "format:";
    public static final String OPTIONS = "options:";
    public static final String LOADS = "loads:";
    public static final String FIELD_DELIMITER = "\u0002";
    public static final String INNER_DELIMITER = "\u0003";

    private String[] _options;
    private String _format;
    private String[] _loads;

    public DataSourceLoader(String line) {
        String[] splitLine = line.split(FIELD_DELIMITER);
        for (String s : splitLine) {
            if (s.startsWith(FORMAT)) {
                _format = s.replaceFirst(FORMAT, ""); // There should only be one format
            } else if (s.startsWith(OPTIONS)) {
                // optionsSplit[0] will just be
                String optionsLine = s.replaceFirst(OPTIONS, "");
                _options = optionsLine.split(INNER_DELIMITER);
            } else if (s.startsWith(LOADS)) {
                String loadsLine = s.replaceFirst(LOADS, "");
                _loads = loadsLine.split(INNER_DELIMITER);
            }
        }
    }

    @Override
    public Dataset<Document> getDocuments() {
        SparkSession sparkSession = getSparkSession();
        DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.format(_format);
        for (int i = 0; i < _options.length; i += 2) {
            String key = _options[i];
            String value = _options[i+1];
            dataFrameReader.option(key, value);
        }
        Dataset<Row> dataset = _loads != null && _loads.length > 0 ? dataFrameReader.load(_loads) : dataFrameReader.load();
        /*
        Dataset<Row> dataset = sparkSession.read()
            .format("jdbc")
            //.format("com.microsoft.sqlserver.jdbc.spark")
            .option("url", "jdbc:sqlserver://localhost;databaseName=playground;integratedSecurity=true;")
            //.option("query", "SELECT note_id AS documentId, note AS text FROM playground.dbo.note")
            .option("query", "SELECT note_id AS documentId, note AS text, patient_id FROM playground.dbo.note")
            //.option("dbtable", "note")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load();
         */
        //Dataset<Row> dataset = sparkSession.sql("SELECT...");

        Dataset<Document> documents = dataset.as(Encoders.bean(Document.class));
        return documents;
    }
}
