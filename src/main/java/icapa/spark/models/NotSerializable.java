package icapa.spark.models;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class NotSerializable implements KryoSerializable {
    private static final long serialVersionUID = 6369241181075151871L;
    public NotSerializable() {
        System.out.println("creating test 2");
    }
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        System.out.println("Serializing object!");
    }
    public String t = "world";

    @Override
    public void write(Kryo kryo, Output output) {
        System.out.println("write");
    }

    @Override
    public void read(Kryo kryo, Input input) {
        System.out.println("read");
    }
}
