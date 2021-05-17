# Spark cTAKES
This is a solution for scaling out cTAKES using Apache Spark. 

## Usage
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

### Create a Custom Document Loader
To create a custom document loader, you need to extend icapa.spark.AbstractLoader and implement getDocuments().

It is easiest to learn with an example, so let's go over creating icapa.spark.LineLoader.
  

### Execute on Spark
The [Spark Documentation](https://spark.apache.org/docs/1.1.0/submitting-applications.html) 
provides excellent context on how to submit your jobs. A bare bones example is provided below:
 * Typical syntax using spark-submit
```
./bin/spark-submit \
  --class <main-class>
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```
Now an example for our application
```
$ ./bin/spark-submit \
  --class CtakesSparkMain \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  target/spark-ctakes-0.1.jar \
  config.properties
```
spark-submit --class icapa.spark.SparkMain --conf spark.jars=C:\root\vdt\icapa\nlp\apache-ctakes-4.0.0.1\lib2\mssql-jdbc-9.2.1.jre8.jar .\spark-ctakes-0.1.jar test-config.properties