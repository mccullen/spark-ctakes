package icapa.spark.functions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import icapa.spark.models.Document;
import org.apache.spark.api.java.function.ForeachFunction;

public class PipelineFunction implements ForeachFunction<Document>, KryoSerializable {

    @Override
    public void call(Document document) {
        System.out.println("");
    }

    @Override
    public void write(Kryo kryo, Output output) {
        System.out.println("");
    }

    @Override
    public void read(Kryo kryo, Input input) {
        System.out.println("");
    }
}
