package icapa.spark.models;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializableClass implements Serializable {
    public SerializableClass() {
        System.out.println("Creating test");
    }
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        System.out.println("Serializing object!");
    }
    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException {
        out.defaultWriteObject();
        System.out.println("Deserializaing");
    }
    public String t = "hello";
}
