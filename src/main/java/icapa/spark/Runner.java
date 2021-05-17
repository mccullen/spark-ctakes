package icapa.spark;

import icapa.spark.loaders.AbstractLoader;
import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.core.resource.FileLocator;
import org.apache.ctakes.dictionary.lookup2.util.JdbcConnectionFactory;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Timer;

public class Runner {
    private static final Logger LOGGER = Logger.getLogger(Runner.class.getName());

    private ConfigurationSettings _config;
    private String[] _args;
    private AbstractLoader _documentLoader;

    public static Runner fromArgs(String[] args) {
        Runner runner = new Runner();
        runner._args = args;
        runner.checkArgumentLength();
        runner._config = Util.getConfigurationSettings(runner._args[0]);
        runner.setDocumentLoader();
        return runner;
    }

    private void checkArgumentLength() {
        if (_args.length != 1) {
            String message = "Application takes exactly one argument.\n" +
                "usage: java -jar spark-ctakes.jar <config-properties-file>";
            throw new RuntimeException(message);
        }
    }
    private void setDocumentLoader() {
        String documentLoaderLine = _config.getDocumentLoader();

        // The first space separates the class name from the parameters
        String[] splitLine = documentLoaderLine.split(" ", 2);
        String documentLoaderName = splitLine[0];
        // The parameters are separated by SOH
        String[] params = splitLine[1].split("\u0001");

        // Create an array of String classes. The loader can only accept ctors with String parameters
        Class[] classes = new Class[params.length];
        Arrays.fill(classes, String.class);
        try {
            Class<?> clazz = Class.forName(documentLoaderName);
            _documentLoader = (AbstractLoader)clazz.getConstructor(classes).newInstance(params);
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            LOGGER.error("Error instantiating document loader", e);
        }
        _documentLoader.init(_config);
    }

    private String getConnectionUrl(String jdbcUrl) {
        String urlDbPath = jdbcUrl.substring("jdbc:hsqldb:file:".length());
        String urlFilePath = urlDbPath + ".script";

        try {
            String fullPath = FileLocator.getFullPath(urlFilePath);
            return fullPath.substring(0, fullPath.length() - ".script".length());
        } catch (FileNotFoundException var4) {
            LOGGER.error("File not found", var4);
        }
        return "";
    }

    public void start() {
        String jdbcUrl = "jdbc:hsqldb:file:resources/org/apache/ctakes/dictionary/lookup/fast/icd/icd";
        String jdbcDriver = "org.hsqldb.jdbcDriver";
        String jdbcUser = "sa";
        String jdbcPass = "";

        String trueJdbcUrl = jdbcUrl;
        if (jdbcUrl.startsWith("jdbc:hsqldb:file:")) {
            trueJdbcUrl = "jdbc:hsqldb:file:" + getConnectionUrl(jdbcUrl);
        }
        LOGGER.info("trueJdbcUrl: " + trueJdbcUrl);

        try {
            Driver driver = (Driver)Class.forName(jdbcDriver).newInstance();
            DriverManager.registerDriver(driver);
        } catch (SQLException var10) {
            LOGGER.error("Could not register Driver " + jdbcDriver, var10);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException var11) {
            LOGGER.error("Could not create Driver " + jdbcDriver, var11);
        }

        LOGGER.info("Connecting to " + jdbcUrl + ":");
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(trueJdbcUrl, jdbcUser, jdbcPass);
        } catch (SQLException var9) {
            LOGGER.error("  Could not create Connection with " + trueJdbcUrl + " as " + jdbcUser, var9);
        }


        // Use broadcast so kryo serialization is used in the closure
        Broadcast<ConfigurationSettings> broadcastConfig = _documentLoader.getJavaSparkContext().broadcast(_config);

        Dataset<Document> documentDataset = _documentLoader.getDocuments();
        // Start up a pipeline for each partition of documents
        documentDataset.foreachPartition(documents -> {
            System.setProperty(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
            JCas jCas = JCasFactory.createJCas();
            PiperFileReader piperFileReader = new PiperFileReader();
            PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
            // Order of methods is important here: set -> load -> build
            pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
            pipelineBuilder.set(ConfigParameterConstants.PARAM_LOOKUP_XML, broadcastConfig.value().getLookupXml());
            piperFileReader.loadPipelineFile(broadcastConfig.value().getPiperFile());
            pipelineBuilder.build();
            AnalysisEngineDescription analysisEngineDescription = pipelineBuilder.getAnalysisEngineDesc();
            //AnalysisEngine analysisEngine = AnalysisEngineFactory.createEngine(analysisEngineDescription);
            AnalysisEngine analysisEngine = UIMAFramework.produceAnalysisEngine(analysisEngineDescription);
            while (documents.hasNext()) {
                Document document = documents.next();
                DocumentID documentID = new DocumentID(jCas);
                documentID.setDocumentID(document.getDocumentId());
                documentID.addToIndexes();
                jCas.setDocumentText(document.getText());
                analysisEngine.process(jCas);
                jCas.reset();
            }
        });
        _documentLoader.getSparkSession().stop();
    }
}
