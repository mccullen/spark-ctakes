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
        String s = "hello there \u0001 world";
        String[] result = s.split(" \u0001 ");
        System.out.println(result);
    }
}
