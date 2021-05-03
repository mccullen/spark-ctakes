package icapa.spark.receivers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Collectors;

import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.internal.ResourceManagerFactory;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.util.CasCreationUtils;

import static java.util.Arrays.asList;

/**
 * @author Selina Chu, Michael Starch, and Giuseppe Totaro
 *
 */
public class CRReceiver extends Receiver<String> {
    public CRReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        try {
            //new Thread(new ReadSocketThread(this.port)).start();
            boolean stopped = isStopped();
            //stop("Test"); // When you stop, it will attempt to restart automatically?
            stopped = isStopped();
            new Thread(() -> {
                try {
                    // TODO
                    /*
                    PiperFileReader piperFileReader = new PiperFileReader();
                    PipelineBuilder pipelineBuilder = piperFileReader.getBuilder();
                    CollectionReaderDescription readerDesc = pipelineBuilder.getReader();
                    ResourceManager resMgr = ResourceManagerFactory.newResourceManager();
                    CollectionReader reader = UIMAFramework.produceCollectionReader(readerDesc, resMgr, null);

                    final CAS cas = CasCreationUtils.createCas(asList(reader.getMetaData(), aae.getMetaData()),
                        null, resMgr);
                    reader.typeSystemInit(cas.getTypeSystem());

                    // Process
                    while (reader.hasNext()) {
                        reader.getNext(cas);
                        //aae.process(cas);
                        cas.reset();
                    }

                     */

                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                store("done");
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStop() {
        System.out.println("stopped");
    }
}
