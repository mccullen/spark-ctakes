package icapa.spark.functions;

import icapa.spark.Util;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;

import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class PartitionFunction implements FlatMapFunction<Iterator<String>, String> {
    private transient AnalysisEngine _aae = null;
    private transient JCas _jCas = null;
    private transient AnalysisEngineDescription _aed = null;
    private int _nInit = 0;
    private int _nCall = 0;

    private void readObject(ObjectInputStream in) {
        try {
            in.defaultReadObject();
            initialize();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // This will get called once per partition
    private void initialize() {
        try {
            ++_nInit;
            System.out.println("N init: " + _nInit);
            /*
             */
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Called once per partition as well
    @Override
    public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
        ++_nCall;

        System.setProperty(UmlsUserApprover.KEY_PARAM, Util.getConfigProperty("umls.key"));
        _jCas = JCasFactory.createJCas();
        PiperFileReader piperFileReader = new PiperFileReader();
        String ctakesHome = System.getenv("CTAKES_HOME");
        //Path path = Paths.get(ctakesHome, "resources/org/apache/ctakes/clinical/pipeline/DefaultFastPipeline.piper");
        Path path = Paths.get(ctakesHome, "default-augmented.piper");
        String location = path.toString();
        piperFileReader.loadPipelineFile(location);
        PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
        pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, Util.getConfigProperty("umls.key"));
        pipelineBuilder.run();

        System.out.println("N Call: " + _nCall);
        return stringIterator;
    }


    // This will get called once per note
    /*
    public String call(String s) throws Exception {
        ++_nCall;
        String xmi = "";
        try {
            _jCas.setDocumentText(s);
            _aae.process(_jCas);
            xmi = Util.getXmi(_jCas);
            _jCas.reset();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return xmi;
    }
     */
}
