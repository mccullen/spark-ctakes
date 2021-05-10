package icapa.spark;

import icapa.spark.models.ConfigurationSettings;
import icapa.spark.models.Document;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Util {
    private static final Logger LOGGER = Logger.getLogger(Util.class.getName());

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
}
