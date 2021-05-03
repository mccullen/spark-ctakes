/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package icapa.spark;

import java.util.Arrays;
import java.util.List;

import icapa.spark.functions.CtakesFunction;
import icapa.spark.models.NotSerializable;
import icapa.spark.models.SerializableClass;
import icapa.spark.receivers.ParagraphReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.uima.jcas.JCas;

/**
 * @author Selina Chu, Michael Starch, and Giuseppe Totaro
 *
 */
public class StreamingMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        Duration batchDuration = new Duration(5000L);
		//int port = Integer.parseInt(args[0]);
		int port = 9321;
		SparkConf sparkConf = new SparkConf()
            .registerKryoClasses(new Class<?>[]{
                SerializableClass.class,
                NotSerializable.class,
                CtakesFunction.class,
                Receiver.class,
                Receiver[].class,
                org.apache.uima.jcas.impl.JCasImpl.class,
                org.apache.uima.cas.impl.CASImpl.class,
                org.apache.uima.cas.impl.FSIndexRepositoryImpl.class,
                java.util.ArrayList[].class,
                java.util.ArrayList.class,
                org.apache.uima.cas.impl.FSIndexRepositoryImpl.class,
                JCas.class,
                //String.class,
                //String[].class,
                ParagraphReceiver.class
            });

        SparkSession ss = SparkSession.builder()
            .appName("app")
            .master("local[3]")
            .config(sparkConf)
			// The kryo serializer requires you to register classes, as above. But it is faster and doesn't
			// require classes to implement Serializable. However, the regular java serializer will still be
			// used in the case of closures (unless it is a broadcast variable). So kyro will be used in cases like
			// sc.parallize(Arrays.asList(new MyClass()...)).foreach(p -> ...)
			// but not in cases where you enclose a variable like
			// sc.parallize(Arrays.asList(new MyClass()...)).foreach(p -> { System.our.print(myClassVariable); })
            // Note that Kryo will NOT call readObject even if the class implements Serializable
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //.config("spark.memory.offHeap.enabled", "true")
            //.config("spark.memory.offHeap.size", "100g")
            /*
             * Use this if you need to register all Kryo required classes.
             * If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
             */
            .config("spark.kryo.registrationRequired", "true")
            .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        // We will get a batch of RDDs from the receiver every x miliseconds
        JavaStreamingContext ssc = new JavaStreamingContext(sc, batchDuration);

        // RDD: Think of it like a list but with functions that can benefit from parallialism

        // dstream: a continuous stream of RDDs. Spark Streaming guarantees ordered processing of rdds
        // but not w/i RDDs. So the rdd for the first batch will get processed first, but the documents
        // within that rdd can be processed in any order. This is how you get the benefit of parallialism.

        // Broadcast: Shared read-only variables that get cached on each node. These are serialized using whichever
        // serialization method you chose such as Kyro. Variable must Serializable if using Java Serialization

        // Think of paragraphs as a list of rdds, each of which contains a list of strings. So it is a list of lists.
        // The outermost list contains the inner lists of documents w/i that batch
        // [[doc1,doc2,doc3], [doc4,doc5,doc6]]
        JavaDStream<String> paragraphs = ssc.receiverStream(new ParagraphReceiver(StorageLevel.MEMORY_AND_DISK_2(), port));
		//JavaDStream<String> paragraphs = ssc.receiverStream(new DocumentReceiver(StorageLevel.MEMORY_AND_DISK_2(), port));
        //JavaDStream<String> paragraphs = ssc.receiverStream(new ReaderReceiver(StorageLevel.MEMORY_AND_DISK_2()));

        // Now, map each paragraph string to xmi
        SerializableClass serializableClass = new SerializableClass();
        serializableClass.t = "changed";
        Broadcast<SerializableClass> b = sc.broadcast(serializableClass);
        NotSerializable t2 = new NotSerializable();
		//JavaDStream<String> xmisInAllRdds = paragraphs.map(new CtakesFunction(b, t2));


        //sc.parallelize(Arrays.asList(JCasFactory.createJCas(), JCasFactory.createJCas())).foreach(p -> {
        //    System.out.println("here");
        //});
        //sc.parallize(Arrays.asList(new MyClass()...)).foreach(p -> ...)
        //Broadcast<SerializableClass> serClassB = sc.broadcast(new SerializableClass());
        SerializableClass serClass = new SerializableClass();
        NotSerializable nonSerClass = new NotSerializable();
        //Broadcast<NotSerializable> nonserClassB = sc.broadcast(new NotSerializable());
        sc.parallelize(Arrays.asList(new SerializableClass(), new SerializableClass())).foreach(p -> {
            //serClassB.value();
            //serClass.t = "new";
            //nonSerClass.t = "new";
            //nonserClassB.value();
            System.out.println(p);
        });
        sc.parallelize(Arrays.asList(new NotSerializable(), new NotSerializable())).foreach(p -> {
            System.out.println(p);
        });

        JavaDStream<String> xmisInAllRdds = paragraphs.map(f -> {
            //System.out.println(t2); // fails, not serializable
            return "";
        });


        // This will get checked every batch duration. Everything stored by the receiver will be part of this batch of rdds
        xmisInAllRdds.foreachRDD(rdd -> {
            // For each rdd in this batch...
            List<String> pars = rdd.collect();
            for(String par:pars) {
                System.out.println(par);
            }
        });

        ssc.start();
        ssc.awaitTermination();
        ssc.close();
	}
}
