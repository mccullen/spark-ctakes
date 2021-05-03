package icapa.spark;

import icapa.spark.functions.MapFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;

public class StressTestMain {
    public static void main(String[] args) throws Exception {
        int nDocuments = 10000;
        if (args.length == 1) {
            nDocuments = Integer.parseInt(args[0]);
            System.out.println("N docs: ");
            System.out.println(nDocuments);
        }
        SparkConf sparkConf = new SparkConf()
            .registerKryoClasses(new Class<?>[]{
            });
        SparkSession ss = SparkSession.builder()
            .appName("app")
            .master("local[4]")
            .config(sparkConf)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // Use this if you need to register all Kryo required classes.
            // If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
            .config("spark.kryo.registrationRequired", "true")
            .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        String text = "Abstract:Cereblon (CRBN), a substrate receptor of the E3 ubiquitin ligase complex CRL4CRBN, is the target of theimmunomodulatory drugs lenalidomide and pomalidomide. Recently, it was demonstrated that binding of thesedrugs to CRBN promotes the ubiquitination and subsequent degradation of two common substrates, transcriptionfactors Aiolos and Ikaros. Here we report that the pleiotropic pathway modifier CC-122, a new chemical entitytermed pleiotropic pathway modifier binds CRBN and promotes degradation of Aiolos and Ikaros in diffuse largeB-cell lymphoma (DLBCL) and T cells in vitro, in vivo and in patients, resulting in both cell autonomous as well asimmunostimulatory effects. In DLBCL cell lines, CC-122-induced degradation or shRNA mediated knockdown ofAiolos and Ikaros correlates with increased transcription of interferon stimulated genes (ISGs) independent ofinterferon production and/or secretion and results in apoptosis in both ABC and GCB-DLBCL cell lines. Ourresults provide mechanistic insight into the cell of origin independent anti-lymphoma activity of CC-122, in contrastto the ABC subtype selective activity of lenalidomide.";
        List<String> texts = new ArrayList<String>();
        for (int i = 0; i < nDocuments; ++i) {
            texts.add(text);
        }
        // Convert list to rdd list
        JavaRDD<String> rdds = sc.parallelize(texts);
        int nPartitions = rdds.getNumPartitions();
        JavaRDD<String> xmiRdds = rdds.map(new MapFunction());
        //JavaRDD<String> xmiRdds = rdds.mapPartitions(new PartitionFunction());

        List<String> xmis = xmiRdds.collect();
        System.out.println("SIZE: " + xmis.size());
        System.out.println("N partitions: " + nPartitions);
        xmis.forEach(d -> {
            System.out.println(d);
        });
        System.out.println("N docs: " + nDocuments);
        sc.close();
        ss.stop();
    }
}
