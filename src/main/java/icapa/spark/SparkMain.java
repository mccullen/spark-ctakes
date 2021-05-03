package icapa.spark;

import icapa.spark.functions.MapFunction;
import icapa.spark.functions.PipelineFunction;
import icapa.spark.functions.PipelineMap;
import icapa.spark.loaders.AbstractDocumentLoader;
import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.codehaus.janino.Java;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SparkMain {
    public static void main(String[] args) {
        ConfigurationSettings config = Util.getConfigurationSettings();
        SparkConf sparkConf = new SparkConf()
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
        SparkSession sparkSession = builder
            .config(sparkConf)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             // Use this if you need to register all Kryo required classes.
             // If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
            .config("spark.kryo.registrationRequired", "true")
            .getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        try {
            Class<?> clazz = Class.forName(config.getDocumentLoader());
            AbstractDocumentLoader documentLoader = (AbstractDocumentLoader)clazz.getConstructor().newInstance();
            documentLoader.init(sparkSession, javaSparkContext);
            Dataset<Document> documentDataset = documentLoader.getDocuments();
            // Use broadcast so kryo serialization is used
            Broadcast<ConfigurationSettings> broadcastConfig = javaSparkContext.broadcast(config);
            documentDataset.foreachPartition(documents -> {
                System.setProperty(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
                JCas jCas = JCasFactory.createJCas();
                PiperFileReader piperFileReader = new PiperFileReader();
                piperFileReader.loadPipelineFile(broadcastConfig.value().getPiperFile());
                PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
                pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, broadcastConfig.value().getUmlsKey());
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
        }

        sparkSession.stop();
    }
}
