package icapa.spark.models;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class ConfigurationSettings {
    private String umlsKey;
    private String documentLoader;
    private String piperFile;
    private String master;
    private String lookupXml;

    public String getLookupXml() {
        return lookupXml;
    }

    public void setLookupXml(String lookupXml) {
        this.lookupXml = lookupXml;
    }

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
