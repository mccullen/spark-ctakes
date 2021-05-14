package icapa.spark.loaders;

import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractLoader {
    private SparkSession _sparkSession;
    private JavaSparkContext _javaSparkContext;
    private ConfigurationSettings _config;

    public void init(ConfigurationSettings config) {
        _config = config;
        setSparkSession();
        _javaSparkContext = JavaSparkContext.fromSparkContext(_sparkSession.sparkContext());
    }

    private void setSparkSession() {
        SparkConf sparkConf = new SparkConf();
        sparkConf
            .registerKryoClasses(new Class<?>[]{
                Document.class,
                ConfigurationSettings.class,
                // Internal classes that Spark should automatically register but don't
                org.apache.spark.sql.catalyst.InternalRow.class,
                org.apache.spark.sql.catalyst.InternalRow[].class
            });
        SparkSession.Builder builder = SparkSession.builder();
        if (_config.getMaster() != null) {
            // If master is provided (like in dev environment), use it. Otherwise, it should be supplied
            // in spark-submit
            builder.master(_config.getMaster());
        }
        _sparkSession = builder
            .config(sparkConf)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // Use this if you need to register all Kryo required classes.
            // If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
            .config("spark.kryo.registrationRequired", "true")
            .getOrCreate();
    }

    public abstract Dataset<Document> getDocuments();

    public SparkSession getSparkSession() {
        return _sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        _sparkSession = sparkSession;
    }

    public JavaSparkContext getJavaSparkContext() {
        return _javaSparkContext;
    }

    public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
        _javaSparkContext = javaSparkContext;
    }

    public ConfigurationSettings getConfigurationSettions() {
        return _config;
    }

    public void setConfigurationSettings(ConfigurationSettings config) {
        _config = config;
    }
}
