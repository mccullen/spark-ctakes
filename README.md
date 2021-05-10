# Spark cTAKES

## Executing on an Existing Spark Cluster
The [Spark Documentation](https://spark.apache.org/docs/1.1.0/submitting-applications.html) provides excellent context on how to submit your jobs. A bare bones example is provided below:
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
  target/spark-ctakes-0.1-job.jar \
  sampledata/all.txt
```
spark-submit --class icapa.spark.SparkMain --conf spark.jars=C:\root\vdt\icapa\nlp\apache-ctakes-4.0.0.1\lib2\mssql-jdbc-9.2.1.jre8.jar .\spark-ctakes-0.1.jar config.properties