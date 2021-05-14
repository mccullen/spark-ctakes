package icapa.spark.loaders;

import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class LineLoader extends AbstractLoader {
    public static final Logger LOGGER = Logger.getLogger(LineLoader.class);

    private String _path;
    private int _startingId;

    public LineLoader(String path) {
        _path = path;
    }

    public LineLoader(String path, String startingId) {
        _path = path;
        _startingId = Integer.parseInt(startingId);
    }

    @Override
    public Dataset<Document> getDocuments() {
        List<Document> documents = new LinkedList<>();
        try {
            Files.lines(Paths.get(_path)).forEach(line -> {
                // for each line in file, create and add the document to the list
                Document document = new Document();
                document.setText(line);
                document.setDocumentId(String.valueOf(_startingId));
                documents.add(document);
                ++_startingId;
            });
        } catch (IOException e) {
            LOGGER.error("Could not open file " + _path, e);
        }
        // Convert list to dataset
        Dataset<Document> documentDataset = getSparkSession()
            .createDataFrame(documents, Document.class)
            .as(Encoders.bean(Document.class));
        return documentDataset;
    }
}
