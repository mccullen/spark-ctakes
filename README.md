# Spark cTAKES
This is a solution for scaling out cTAKES using Apache Spark. 

## Usage
### Prerequisites
Before you can submit your job over spark-submit, you will need to satisfy these prerequisites
- Download the latest release jar [here](https://github.com/mccullen/spark-ctakes/releases)
- Our solution was tested on spark 3.1.1, scala 2.12.10, and hadoop 2.7. 
- All nodes must be able to access cTAKES
- All nodes must set the CTAKES_HOME environment variable to point to cTAKES

In our implementation over Amazon EMR, we stored cTAKES on Amazon EFS. If using a shared copy of cTAKES, you MUST
edit your cTAKES dictionaries to allow multiple pipelines to access it in parallel. To do this, you need to 
disable the lock file on the dictonary properties and make it readonly. For example, to do this to the sno_rx_16ab
dictionary (the default used by cTAKES), go to
```
resources/org/apache/ctakes/dictionary/lookup/fast/sno_rx_16ab/sno_rx_16ab.properties
```
and add the readonly and hsqldb.lock_file properties so your file should look something like
```properties
#HSQL Database Engine 2.3.4
#Tue Apr 04 21:01:14 EDT 2017
version=2.3.4
modified=no
tx_timestamp=0
readonly=true
hsqldb.lock_file=false
```

### Configuration
To use, you will need to specify some configuration properties in a config.properties file following the convention below
```properties
piper.file=my-piper-file.piper
document.loader=MyLoader arg1\u0001arg2
umls.key=123-123-123-123
lookup.xml=org/apache/ctakes/dictionary/lookup/fast/icd.xml
```

Here is an explaination of each property
- **piper.file**: The path to the piper file to use for your NLP pipeline. The path can be absolute or relative to
CTAKES_HOME. The piper file **MUST NOT** have a 
collection reader (CR) defined (see FYI below). 
  - **FYI**: The reason you cannot specify a CR is because Spark works by converting a list into an RDD,
  which is basically a distributed collection that Spark uses to partition work on its elements to worker nodes.
  In our case, this would be a list of notes, but a CR just keeps calling readNext(jcas) until there are no more
  documents left. So you do not have the list of notes beforehand and so cannot use it to create an RDD of note strings.
  A possible alternative would be to create the CR and call readNext() manually just to extract the document text
  from the CAS and append to a list, but that may not scale well if the list was very large. Also, if you were
  just creating a dummy CAS for the CR to extract the document text, then any types added by the CR to the CAS 
  would not be there in the CAS that the worker node used for the rest of the pipeline. So the best solution I've
  found is to have the user define a loader class (described below) that reads in the documents
- **document.loader**: The document loader class to read in documents. Think of this as the replacement for the 
CR. We provide some of our own document loaders for you to use but you can create your own custom loader if necessary. 
The full name of the class must be provided first followed by a space and then the arguments to its constructor 
(which muse all be of type String)
delimited by \u0001, which is a non-typeable UTF-16 code so it doesn't matter if your argument values
contain spaces. 
- **umls.key**: Your umls api key
- **lookup.xml**: Path (relative to CTAKES_HOME) to the lookup xml file for the dictionary you want to use for your
pipeline.

### Execute on Spark
Once you have your configuration ready, you can submit your spark job using spark-submit. Here is an example for
our application:
```
./bin/spark-submit 
  --master spark://207.184.161.138:7077
  --class icapa.spark.SparkMain 
  --conf spark.jars=/tmp/custom-components-1.0.jar 
  --conf spark.executor.memory=16g 
  --conf spark.driver.memory=16g 
  --conf "spark.yarn.appMasterEnv.CTAKES_HOME=/efs/apache-ctakes-4.0.0.1" \
  --conf "spark.executorEnv.CTAKES_HOME=/efs/apache-ctakes-4.0.0.1" \
  ./spark-ctakes-0.1.jar config.properties
```

Here is an explanation of each argument and configuration setting:
- **master**: The master url of your cluster
- **class**: The entry point (should always be icapa.spark.SparkMain)
- **spark.jars**: Conf setting to sepcify additional jars to add to the classpath of the driver and executor nodes.
This is only necessary if you have some custom UIMA components or document loaders in another jar.
- **spark.executor.memory**: The amount of memory to use for each executor process. We need to increase this from the
default or else we will run into out of heap memory errors since cTAKES uses a lot of memory.
- **spark.driver.memory**: The same as spark.executor.memory but for the driver node.
- **spark-ctakes-0.1.jar config.properties**: The application jar with the main class (SparkMain) followed by the 
path to your config.properties file (absolute or relative to your current working directory).
- **spark.yarn.appMasterEnv.CTAKES_HOME**: The path to CTAKES_HOME on the master node. This is used by the ctakes
file locator to find various resources (dictionaries, model files, etc.)
- **spark.yarn.executorEnv.CTAKES_HOME**: The path to CTAKES_HOME on the executor nodes. This is used by the ctakes
file locator to find various resources (dictionaries, model files, etc.)

See the [Spark Documentation](https://spark.apache.org/docs/1.1.0/submitting-applications.html) for more info
on submitting spark applications. Also, see [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) 
for a description of all the --conf configuration settings.

That's it! 

## Document Loaders
Our spark-ctakes solution comes with a few document loaders. These are classes to read in documents. Think of this as the replacement for the 
CR. We provide some of our own document loaders for you to use but you can create your own custom loader if necessary. 
### icapa.spark.loaders.DataSourceLoader
This document loader lets you read in documents from a variety of formats accepted by Spark SQL (jdbc, parquet, csv, json etc.).
The configuration you would set in your config.properties file would look something like this:

```properties
document.loader=icapa.spark.loaders.DataSourceLoader format:csv\u0002options:sep\u0003,\u0003inferSchema\u0003true\u0003header\u0003true\u0002examples/src/main/resources/notes.csv
```
There is only one argument to DataSourceLoader and it is a configuration string for specifying the format, options, and load paths.
Below is an explaination of each section of the configuration string:
- format: The Spark SQL compatible format (jdbc, csv, json, etc.)
- options: The options to set for configuring the data frame reader (delimited by \u0003 for multiple options). If not provided,
no options will be set.
- loads: The load paths to set for configuring the data frame reader (delimited by \u0003 for multiple paths). If not provided, no load paths
will be set and it will use load() without specifying any paths (useful in jdbc, for example, where there is no path and the DB connection
settings are configured by the options).

Note that, when the dataset is loaded in, it will be cast to the Document class, which as a documentId and text property. Thus,
you must ensure that your column names conform to this standard or the cast will fail.

Here is an example of how you may connect using the jdbc format. Note that there is no loads specified since we are connecting to the DB rather
than loading in a file. Also note that we rename our columns in our query as documentId and text in order to conform to the Document class
properties.

```properties
document.loader=icapa.spark.loaders.DataSourceLoader format:jdbc\u0002options:url\u0003jdbc:sqlserver://localhost;integratedSecurity=true;\u0003driver\u0003com.microsoft.sqlserver.jdbc.SQLServerDriver\u0003query\u0003SELECT TOP 10 note_id AS documentId, note AS text FROM playground.dbo.note
```

## Create a Custom Document Loader
To create a custom document loader, you need to extend icapa.spark.AbstractLoader and implement getDocuments().

It is easiest to learn with an example, so let's go over creating icapa.spark.LineLoader. This loader is pretty simple.
It just loads in Documents (one per line) from a file given
 - the path to the file (absolute or relative to the current working directory)
 - the starting document id (optional. If absent it will default to 0. This means that the first document will have
   a DocumentId annotation with and id of "0" added to the CAS, then next will have one of "1", and so on)

Here is our implementation:

```java
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
```

Check the comments and code for details. Here are the key takeaways:
- You **MUST** extend AbstractLoader
- You **MUST** override getDocuments, which returns your Dataset of Documents 

You will now be able to set this as your document loader in your config.properties file:
```properties
document.loader=icapa.spark.loaders.LineLoader path/to/notes.txt\u0001100
```
Alternatively, if you want to just use the default starting document id instead of 100:
```properties
document.loader=icapa.spark.loaders.LineLoader path/to/notes.txt
```

Then, when you do spark-submit, just make sure that you include the path to the jar containing this class in the 
spark.jars configuration setting:
```
--conf spark.jars=/path/to/jar/with/line-loader.jar 
```
