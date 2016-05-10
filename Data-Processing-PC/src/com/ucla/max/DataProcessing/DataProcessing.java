package com.ucla.max.DataProcessing;

import java.util.HashMap;
import java.util.Map;

import java.io.*;
import java.net.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
// import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;

import scala.Tuple2;

public class DataProcessing {
	
	public static String result = ""; // analysis result sending back to Android device
	
	@SuppressWarnings("deprecation")
	public static void processKafkaData() {
		
	    SparkConf sparkConf = new SparkConf().setAppName("processKafkaData").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.driver.port", "9095");
	    	// Create the context with 2 seconds batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000)); // "Duration" imported to be in "org.apache.spark.streaming"
		
	    // get messages of key-value pairs by Kafka consumer
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    topicMap.put("temperature", 1);
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "127.0.0.1", "tempSensor", topicMap); // consumer group ID: tempSensor; per-topic number of Kafka partitions to consume: "temperature" <-> 1
		
		// parse the message to get values only
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() { // "Function" imported to be in "org.apache.spark.api.java.Function"
	        @Override
	        public String call(Tuple2<String, String> tuple2) {
	          return tuple2._2();
	        }
	      });
	    
	    // if we can get each JavaRDD inside the JavaDStream, we can apply the following to process temperature data.
	    /*
	    JavaRDD<String> dataSet;
	    int count = lines.map(new Function<String, Integer>() {
	    	@Override
	    	public Integer call(String string) {
	    		return (Integer.parseInt(string) >= 90) ? 1 : 0;
	    	}
	    }).reduce(new Function2<Integer, Integer, Integer>() {
	    	@Override
	    	public Integer call(Integer integer, Integer integer2) {
	    		return (integer + integer2);
	    	}
	    });
	    if (count > 0)
	    	System.out.printf("Today is hot! There is at least one temperature data greater than 90 degree.\n");
	    else 
	    	System.out.printf("A normal day.\n");
	    */
	    
	    // Rule #1: check if any temperature sensor data is above a threshold
	    // trying process JavaDStream directly, without foreachRDD()
	    JavaDStream<Integer> count = lines.map(new Function<String, Integer>() {
	    	@Override
	    	public Integer call(String string) {
	    		System.out.printf("Received a JavaDStream; temperature = %d.\n", Integer.parseInt(string));
	    		return (Integer.parseInt(string) >= 90) ? 1 : 0;
	    	}
	    }).reduce(new Function2<Integer, Integer, Integer>() {
	    	@Override
	    	public Integer call(Integer integer, Integer integer2) {
	    		return (integer + integer2);
	    	}
	    });
	    
	    count.foreachRDD(
	    	new Function2<JavaRDD<Integer>, Time, Void>() {
	    		@Override
	    		public Void call(JavaRDD<Integer> dataSet, Time time) {
	    			Integer num = dataSet.first();
    			    if (num > 0) {
    			    	System.out.printf("Today is hot! There is at least one temperature data greater than 90 degree.\n");
    			    	result += "Today is hot! There is at least one temperature data greater than 90 degree. ";
    			    }
//    			    else 
//    			    	System.out.printf("Data showing a normal day.\n");
    			    return null;
	    		}
	    	}
	    );
	    
	    // Rule #2: Check if any temperature sensor data is below a threshold
	    JavaDStream<Integer> count2 = lines.map(new Function<String, Integer>() {
	    	@Override
	    	public Integer call(String string) {
//	    		System.out.printf("Received a JavaDStream; temperature = %d.\n", Integer.parseInt(string));
	    		return (Integer.parseInt(string) <= 32) ? 1 : 0;
	    	}
	    }).reduce(new Function2<Integer, Integer, Integer>() {
	    	@Override
	    	public Integer call(Integer integer, Integer integer2) {
	    		return (integer + integer2);
	    	}
	    });
	    
	    count2.foreachRDD(
	    	new Function2<JavaRDD<Integer>, Time, Void>() {
	    		@Override
	    		public Void call(JavaRDD<Integer> dataSet, Time time) {
	    			Integer num = dataSet.first();
    			    if (num > 0) {
    			    	System.out.printf("Today is cold! There is at least one temperature data lower than 32 degree.\n");
    			    	result += "Today is cold! There is at least one temperature data lower than 32 degree.";
    			    }
//    			    else 
//    			    	System.out.printf("Data showing a normal day.\n");
    			    sendBackResult();
    			    
    			    return null;
	    		}
	    	}
	    );
	    

//	    System.out.printf("Preparing to process lines.foreachRDD. \n");
	    
//	    while (true) {
//	    	System.out.printf("In infinite loop...\n");
	    /*
    	lines.foreachRDD(
    		new Function2<JavaRDD<String>, Time, Void>() { // "Time" imported to be in "org.apache.spark.streaming.Time"
    			@Override
    			public Void call(JavaRDD<String> dataSet, Time time) {
    				
    				System.out.printf("Starting to process one javaRDD!\n");
    				
    				int count = dataSet.map(new Function<String, Integer>() {
    			    	@Override
    			    	public Integer call(String string) { // if the temperature string is greater than 90, add an element 1 to the new Integer array; otherwise add 0 to the new array.
    			    		return (Integer.parseInt(string) >= 90) ? 1 : 0;
    			    	}
    			    }).reduce(new Function2<Integer, Integer, Integer>() {
    			    	@Override
    			    	public Integer call(Integer integer, Integer integer2) { // sum up all numbers in the new Integer array
    			    		return (integer + integer2);
    			    	}
    			    });
    				
    			    if (count > 0)
    			    	System.out.printf("Today is hot! There is at least one temperature data greater than 90 degree.\n");
    			    else 
    			    	System.out.printf("A normal day.\n");
    			    
    			    return null;
    			}
    		}
    	);
    	*/
    	
        jssc.start();
        jssc.awaitTermination();
        
    }
	
	public static void sendBackResult() {
		Socket mySocket = null;
        DataOutputStream os = null;
        // DataInputStream is = null;
        BufferedReader is = null;

        try {
            mySocket = new Socket("131.179.30.188", 9930);
            os = new DataOutputStream(mySocket.getOutputStream());
            // is = new DataInputStream(mySocket.getInputStream());
            is = new BufferedReader(new InputStreamReader(mySocket.getInputStream()));
        } catch (UnknownHostException exception) {
            System.out.println(exception);
        } catch (IOException exception) {
            System.out.println(exception);
        }

        if (mySocket != null && os != null && is != null) {
            try {
//                os.writeBytes("Lo and Behold!\n");
//                os.writeBytes("QUIT");
                os.writeBytes(result);
//                for (int i = 0; i < DATA_COUNT; i++) {
//                    Log.d("sunnyDay", "Sending messages to server...");
//                    os.writeInt(num[i]);
//                }

                // not expecting reply from the server (Android device), so directly close the Socket.
                /* 
                String responseLine;
                while ((responseLine = is.readLine()) != null) {
                    String message = "Got server reply: " + responseLine;
                }
				*/
                
               	System.out.printf("Closing the Socket...\n");
                os.close();
                is.close();
                mySocket.close();
            }  catch (IOException exception) {
                System.out.println(exception);
            }
        }
    }
	
}

	
	
//	}
	
	/*
	public static void kafkaIntegration() {
		// JavaPairReceiverInputDStream<String, String> KafkaStream = KafkaUtils.createStream(streamingContext, [ZK quorum], "tempSensor", 1); // consumer group ID: tempSensor; per-topic number of Kafka partitions to consume: 1
		
	}
	
	public static void sparkStreamingExample() {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1))
	}
	
	private static final Pattern SPACE = Pattern.compile(" ");

	private JavaKafkaWordCount() {
	}
	
	  public static void main(String[] args) {
		    if (args.length < 4) {
		      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
		      System.exit(1);
		    }

		    StreamingExamples.setStreamingLogLevels();
		    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		    // Create the context with 2 seconds batch size
		    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

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

		    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
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
		    jssc.start();
		    jssc.awaitTermination();
	}
	*/
