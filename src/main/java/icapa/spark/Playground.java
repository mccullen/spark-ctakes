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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Playground {
    public static void main(String[] args) throws Exception {
        File f = new File("test.props");
        if (f.exists()) {
            System.out.println("exists");
        } else {
            System.out.println("Does not exist");
        }
    }
}
