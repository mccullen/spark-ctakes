package icapa.spark;

import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.JCasPool;

import java.util.ArrayList;
import java.util.List;

public class Playground {
    public static void main(String[] args) throws Exception {
        JCas jCas = JCasFactory.createJCas();

        PiperFileReader piperFileReader = Util.getDefaultPiper();
        AnalysisEngineDescription analysisEngineDescription = piperFileReader.getBuilder().getAnalysisEngineDesc();
        AnalysisEngine aae = AnalysisEngineFactory.createEngine(analysisEngineDescription);
        JCasPool jCasPool = new JCasPool(10, aae);
        //UIMAFramework.produceAnalysisEngine(analysisEngineDescription);
        int nDocuments = 10;
        if (args.length == 1) {
            nDocuments = Integer.parseInt(args[0]);
            System.out.println("N docs: ");
            System.out.println(nDocuments);
        }
        String memory = "16g";
        String classpath = "C:/root/vdt/icapa/nlp/apache-ctakes-4.0.0.1/lib/*";
        SparkConf sparkConf = new SparkConf()
            .registerKryoClasses(new Class<?>[]{
                Integer.class,
                Thread.class,
                Integer[].class,
                scala.collection.mutable.WrappedArray.ofRef.class,
                Object[].class,
                Thread[].class
            });
        SparkSession ss = SparkSession.builder()
            .appName("app")
            .master("local[4]")
            .config(sparkConf)
            .config("spark.executor.memory", memory)
            .config("spark.driver.memory", memory)
            .config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrationRequired", "true")
            .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        List<Integer> l = new ArrayList<>();
        l.add(1);l.add(2);l.add(3);l.add(5);
        JavaRDD<Integer> r = sc.parallelize(l);
        for (int i = 0; i < 100; ++i) {
            System.out.println("loop");
            r.foreach(a -> {
                System.out.println("Going to sleep");
                for (int k = 0; k < 10000000; ++k) {
                }
                System.out.println("done sleeping");
            });
            System.out.println("end");
        }
        sc.close();
        ss.stop();
    }
}
