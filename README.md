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
spark-submit --class SparkMain --master local[*] .\spark-ctakes-0.1-jar-with-dependencies.jar 10