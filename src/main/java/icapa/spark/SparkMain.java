package icapa.spark;

import com.lexicalscope.jewel.cli.CliFactory;
import icapa.spark.loaders.AbstractLoader;
import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.CliOptionals;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;

import java.util.Arrays;

public class SparkMain {
    public static void main(String[] args) {
        OrcFileFormat orc = new OrcFileFormat();
        ConfigurationSettings config = Util.getConfigurationSettings();
        SparkConf sparkConf = new SparkConf();
        sparkConf
            .registerKryoClasses(new Class<?>[]{
                Document.class,
                ConfigurationSettings.class
            });
        SparkSession.Builder builder = SparkSession.builder();
        if (config.getMaster() != null) {
            // If master is provided (like in dev environment), use it. Otherwise, it should be supplied
            // in spark-submit
            builder.master(config.getMaster());
        }
        String driverLoc = "C:/root/vdt/icapa/nlp/apache-ctakes-4.0.0.1/lib/mssql-jdbc-9.2.1.jre8.jar";
        SparkSession sparkSession = builder
            .config(sparkConf)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             // Use this if you need to register all Kryo required classes.
             // If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
            .config("spark.kryo.registrationRequired", "true")
            .config("spark.executor.extraClassPath", driverLoc)
            .config("spark.driver.extraClassPath", driverLoc)
            .config("spark.jars", driverLoc)
            .getOrCreate();

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        javaSparkContext.addFile(driverLoc);
        javaSparkContext.addJar(driverLoc);
        try {
            String documentLoaderLine = config.getDocumentLoader();
            String[] splitLine = documentLoaderLine.split(" ", 2);
            String documentLoaderName = splitLine[0];
            String[] params = splitLine[1].split("\u0001");
            Class[] classes = new Class[params.length];
            Arrays.fill(classes, String.class);
            Class<?> clazz = Class.forName(documentLoaderName);
            AbstractLoader documentLoader = (AbstractLoader)clazz.getConstructor(classes).newInstance(params);
            documentLoader.init(sparkSession, javaSparkContext);
            Dataset<Document> documentDataset = documentLoader.getDocuments();
            // Use broadcast so kryo serialization is used
            Broadcast<ConfigurationSettings> broadcastConfig = javaSparkContext.broadcast(config);
            documentDataset.foreachPartition(documents -> {
                System.setProperty(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
                JCas jCas = JCasFactory.createJCas();
                PiperFileReader piperFileReader = new PiperFileReader();
                String[] optionsArgs = {
                    "--key", broadcastConfig.value().getUmlsKey(),
                    "-p", broadcastConfig.value().getPiperFile(),
                    "-l", broadcastConfig.value().getLookupXml()
                };
                CliOptionals options = CliFactory.parseArguments(CliOptionals.class, optionsArgs);
                piperFileReader.loadPipelineFile(broadcastConfig.value().getPiperFile());
                piperFileReader.setCliOptionals(options);
                PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
                pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
                pipelineBuilder.set(ConfigParameterConstants.PARAM_LOOKUP_XML, options.getLookupXml());

                pipelineBuilder.build();
                AnalysisEngineDescription analysisEngineDescription = pipelineBuilder.getAnalysisEngineDesc();
                AnalysisEngine analysisEngine = AnalysisEngineFactory.createEngine(analysisEngineDescription);
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
            //System.out.println(documentLoader);
            //documentDataset.collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        sparkSession.stop();
    }
}
