package icapa.spark.loaders;

import icapa.spark.models.Document;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Loads in Documents (one per line) from a file given
 *  - the path to the file (absolute or relative to the current working directory)
 *  - the starting document id (optional. If absent it will default to 0. This means that the first document will have
 *    a DocumentId annotation with and id of "0" added to the CAS, then next will have one of "1", and so on)
 */
public class LineLoader extends AbstractLoader { // NOTE: We are extending AbstractLoader
    public static final Logger LOGGER = Logger.getLogger(LineLoader.class);

    private String _path; // path to the file to read in line by line (one line per document)
    private int _startingId; // defaults to 0. Determines the starting DocumentID

    /**
     * Creates a LineLoader that will read from the file defined in path. Starting id will default to 0.
     *
     * @param path path to the file to load (relative to current work directory or absolute).
     */
    public LineLoader(String path) {
        _path = path;
    }

    /**
     * Creates a LineLoader that will read from the file defined in path and initializes the starting id.
     *
     * @param path path to the file to load (relative to current work directory or absolute).
     * @param startingId the starting DocumentID
     */
    public LineLoader(String path, String startingId) {
        _path = path;
        _startingId = Integer.parseInt(startingId);
    }

    /**
     * You MUST override this function. This function will retirn your Datset of Documents. A document is just
     * a model object with a DocumentId (String) and Text (String). The DocumentId will be used to add
     * a DocumentID annotation to the CAS. The text will be used to set the document text on the CAS.
     */
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
