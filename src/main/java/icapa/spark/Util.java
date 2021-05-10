package icapa.spark;

import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.ctakes.dictionary.lookup2.util.UmlsUserApprover;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XMLSerializer;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Util {
    private static final Logger LOGGER = Logger.getLogger(Util.class.getName());
    public static String getXmi(JCas jcas) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
        boolean formatted = false;
        XMLSerializer xmlSerializer = new XMLSerializer(baos, formatted);
        xmiSerializer.serialize(jcas.getCas(),xmlSerializer.getContentHandler());
        return baos.toString("utf-8");
    }

    public static PiperFileReader getDefaultPiper() {
        System.setProperty(UmlsUserApprover.KEY_PARAM, getConfigProperty("umls.key"));
        PiperFileReader piperFileReader = new PiperFileReader();
        try {
            String ctakesHome = System.getenv("CTAKES_HOME");
            Path path = Paths.get(ctakesHome, "resources/org/apache/ctakes/clinical/pipeline/DefaultFastPipeline.piper");
            String location = path.toString();
            piperFileReader.loadPipelineFile(location);
            PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
            pipelineBuilder.set(UmlsUserApprover.KEY_PARAM, getConfigProperty("umls.key"));
            //pipelineBuilder.build();
            //AnalysisEngineDescription aed = pipelineBuilder.getAnalysisEngineDesc();
            //AnalysisEngine analysisEngine = AnalysisEngineFactory.createEngine(aed);
        } catch (Exception ex) {
            LOGGER.error("Error loading default pipeline", ex);
        }
        return piperFileReader;
    }

    public static ConfigurationSettings getConfigurationSettings(String path) {
        ConfigurationSettings configurationSettings = new ConfigurationSettings();
        File file = new File(path);
        if (file.isFile()) {
            try {
                InputStream input = new FileInputStream(file);
                Properties props = new Properties();
                props.load(input);
                configurationSettings.setUmlsKey(props.getProperty("umls.key"));
                configurationSettings.setPiperFile(props.getProperty("piper.file"));
                configurationSettings.setDocumentLoader(props.getProperty("document.loader"));
                configurationSettings.setMaster(props.getProperty("master"));
                configurationSettings.setLookupXml(props.getProperty("lookup.xml"));
            } catch (Exception e) {
                LOGGER.error("Error loading configuration settings from " + path, e);
            }
        }
        return configurationSettings;
    }

    public static String getConfigProperty(String key) {
        String result = "";
        try (InputStream input = Util.class.getClassLoader().getResourceAsStream(Const.CONFIG_FILE)) {
            Properties props = new Properties();
            props.load(input);
            result = props.getProperty(key);
        } catch (IOException ex) {
            LOGGER.error("Error loading configuration settings", ex);
        }
        return result;
    }

    public static List<Document> getTestDocuments(int nDocuments) {
        String text = "Abstract:Cereblon (CRBN), a substrate receptor of the E3 ubiquitin ligase complex CRL4CRBN, is the target of theimmunomodulatory drugs lenalidomide and pomalidomide. Recently, it was demonstrated that binding of thesedrugs to CRBN promotes the ubiquitination and subsequent degradation of two common substrates, transcriptionfactors Aiolos and Ikaros. Here we report that the pleiotropic pathway modifier CC-122, a new chemical entitytermed pleiotropic pathway modifier binds CRBN and promotes degradation of Aiolos and Ikaros in diffuse largeB-cell lymphoma (DLBCL) and T cells in vitro, in vivo and in patients, resulting in both cell autonomous as well asimmunostimulatory effects. In DLBCL cell lines, CC-122-induced degradation or shRNA mediated knockdown ofAiolos and Ikaros correlates with increased transcription of interferon stimulated genes (ISGs) independent ofinterferon production and/or secretion and results in apoptosis in both ABC and GCB-DLBCL cell lines. Ourresults provide mechanistic insight into the cell of origin independent anti-lymphoma activity of CC-122, in contrastto the ABC subtype selective activity of lenalidomide.";
        List<Document> texts = new ArrayList<>();
        for (int i = 0; i < nDocuments; ++i) {
            Document doc = new Document();
            doc.setDocumentId(String.valueOf(i));
            doc.setText(text);
            texts.add(doc);
        }
        return texts;
    }

    public static String getJarPath() {
        String jarPath = "";
        try {
            jarPath = new File(Util.class.getProtectionDomain().getCodeSource().getLocation()
                .toURI()).getPath();
        } catch (URISyntaxException e) {
            LOGGER.error("Error getting path to jar", e);
        }
        return jarPath;
    }

    public static String getJarDir() {
        String jarPath = Util.getJarPath();
        Path path = Paths.get(jarPath);
        String jarDir = path.getParent().toString();
        return jarDir;
    }
}
