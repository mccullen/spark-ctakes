package icapa.spark.receivers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * @author Selina Chu, Michael Starch, and Giuseppe Totaro
 *
 */
public class DocumentReceiver extends Receiver<String> {
    private static final long serialVersionUID = 1L;
    private int port = 0;
    public DocumentReceiver(StorageLevel storageLevel, int port) {
        super(storageLevel);
        // TODO Auto-generated constructor stub
        this.port = port;
    }

    @Override
    public void onStart() {
        // TODO Auto-generated method stub
        try {
            new Thread(new ReadSocketThread(this.port)).start();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void onStop() {
        // TODO Auto-generated method stub

    }

    class ReadSocketThread implements Runnable {
        ServerSocket socket = null;
        public ReadSocketThread(int port) throws IOException {
            this.socket = new ServerSocket(port);
        }
        @Override
        public void run() {
            Socket client = null;
            BufferedReader buffer = null;
            while (!DocumentReceiver.this.isStopped()) {
                try {
                    if (client == null || !client.isConnected()) {
                        client = this.socket.accept();
                        buffer = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    }
                    // result will be null if socket connection is closed
                    String result = buffer.readLine();
                    if (result != null) {
                        int length = Integer.parseInt(result);
                        OutputStream outputStream = client.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        if (length == -1) {
                            printWriter.print("done");
                            System.exit(0);
                        }
                        char[] buf = new char[length];
                        buffer.read(buf);
                        printWriter.print("recieved");
                        printWriter.flush();
                        String s = new String(buf);
                        if (s != null) {
                            DocumentReceiver.this.store(s);
                        }
                        else {
                            try {
                                buffer.close();
                                client.close();
                            }
                            catch(IOException ee) {
                                ee.printStackTrace();
                            }
                            client = null;
                        }
                    }
                    else {
                        buffer.close();
                        client.close();
                        client = null;
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                    try {
                        buffer.close();
                        client.close();
                    }
                    catch(IOException ee) {
                        ee.printStackTrace();
                    }
                    client = null;
                }

            }
            // TODO Auto-generated method stub

        }

    }

}
