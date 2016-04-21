/*
 * Example Java code for integrating Kafka consumer with Spark Streaming and Hbase
 */

package examples.kafka_spark_hbase;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.cloudera.spark.hbase.JavaHBaseContext;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Consumes messages from one or more topics in Kafka, process and ingests to hbase.
 * Assuming the messages in Kafka is comma separated two strings (key  & value for hbase)
 *
 * Usage: JavaKafkaSparkHbaseExample <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example JavaKafkaSparkHbaseExample zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class JavaKafkaSparkHbaseExample {
  private static final Pattern SPACE = Pattern.compile(" ");

  private JavaKafkaSparkHbaseExample() {
  }

  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaSparkHbaseExample <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    //StreamingExamples.setStreamingLogLevels();
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaSparkHbaseExample");
    //sparkConf.set("spark.driver.allowMultipleContexts","true");
    //sparkConf.set("spark.cleaner.ttl", "120000");
    
    // Create the context with a 10 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(30000));
    //jssc.checkpoint("checkpoint1");

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });

    /*JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(SPACE.split(x));
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    wordCounts.print();
    */
    //Save to Hbase
    //JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    System.out.println("Writing to Hbase...");
    Configuration conf = HBaseConfiguration.create();
    //conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jssc.sparkContext(), conf);

    hbaseContext.streamBulkPut(lines, "t1", new PutFunction(), false);  
    jssc.start();
    
    jssc.awaitTermination();
  }
  
  public static class PutFunction implements Function<String, Put> {

	    private static final long serialVersionUID = 1L;

	    public Put call(String str) throws Exception {
	      String[] kvp = str.split(",");
	      Put put = new Put(Bytes.toBytes(kvp[0]));
	      put.add(Bytes.toBytes("f1"),Bytes.toBytes("c1"),Bytes.toBytes(kvp[1]));
	      return put;
	 }
  }
}
