package icapa.spark.receivers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Collectors;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * @author Selina Chu, Michael Starch, and Giuseppe Totaro
 *
 */
public class ReaderReceiver extends Receiver<String> {
    public ReaderReceiver(StorageLevel storageLevel) {
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
                for (int i = 0; i < 3; ++i) {
                    String test = "traumatic brain injury was reported by the patient on monday";
                    store(test);
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
