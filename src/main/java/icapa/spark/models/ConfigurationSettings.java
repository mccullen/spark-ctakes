package icapa.spark.models;

public class ConfigurationSettings {
    private String umlsKey;
    private String documentLoader;
    private String piperFile;
    private String master;

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getUmlsKey() {
        return umlsKey;
    }

    public void setUmlsKey(String umlsKey) {
        this.umlsKey = umlsKey;
    }

    public String getDocumentLoader() {
        return documentLoader;
    }

    public void setDocumentLoader(String documentLoader) {
        this.documentLoader = documentLoader;
    }

    public String getPiperFile() {
        return piperFile;
    }

    public void setPiperFile(String piperFile) {
        this.piperFile = piperFile;
    }
}
