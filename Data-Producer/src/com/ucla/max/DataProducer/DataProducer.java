package com.ucla.max.DataProducer;

import java.io.*;
import java.net.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataProducer {
	
//	public static Integer[] num = {14, 32, 50, 68, 86, 90, 108};
	public static int DATA_COUNT = 10;
	public static Integer[] num = new Integer[DATA_COUNT];
	
	public static String temperature = ""; // a String for receiving temperature data from Android device
	
    public static String PC_IP = "131.179.30.42";
    public static String ANDROID_IP = "131.179.45.175";
    public static Integer PORT = 9940;
	
	public static void startProducer() {
		// Use Kafka producer to send the temperature data coming from Android device to Kafka server
		 System.out.printf("startProducer() called.\n");
		
		 // initialize Kafka producer
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092"); // producer port 9092
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 // send data to Kafka server
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 0; i < DATA_COUNT; i++) {
			 System.out.printf("Sending data... num = %d\n", num[i]);
		     producer.send(new ProducerRecord<String, String>("temperature", Integer.toString(i), Integer.toString(num[i]))); // send to topic "temperature"
		 }
		 
		 producer.close();
	}
	
	public static void startServer() {
		// initialize a Socket for TCP/IP communication with Android device.
		// Receiving temperature data from Android.
		System.out.printf("Preparing to start server...\n");
		
		ServerSocket echoServer = null;
        String line;
        // DataInputStream is;
        BufferedReader is;
        // PrintStream os;
        Socket clientSocket = null;

        System.out.printf("Initializing Socket...\n");
        try {
           echoServer = new ServerSocket(PORT);
        } catch (IOException e) {
           System.out.println(e);
        }   

	    try {
	    	   // waiting for client connection
	           clientSocket = echoServer.accept();
	           // is = new DataInputStream(clientSocket.getInputStream());
	           is = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//	           os = new PrintStream(clientSocket.getOutputStream());
	           
	           System.out.printf("Connection with client established. Listening for incoming messages...\n");
	           while (true) {
	             line = is.readLine();
//	             os.println(line); 
//	             System.out.printf("Echoed the message from client.\n");
	             
	             temperature = line; // receive the String from Android for temperature data
	             System.out.printf("Received temperature data of String format. Closing server...\n");
	             break;
	           }
	     } catch (IOException e) {
	           System.out.println(e);
	     }
	    
	    System.out.printf("Closing sockets...");
	    try {
	        clientSocket.close();
	        echoServer.close();
	    } catch (IOException exception) {
	    	System.out.println(exception);
	    }
		
	}
	
	public static void parseTemperature() {
		// parse the incoming temperature data from String to Integer array.
		String[] parsing = temperature.split(", ");
		for (int i = 0; i < DATA_COUNT; i++) {
			num[i] = Integer.parseInt(parsing[i]);
		}
		
		System.out.printf("Temperature data array generated: ");
		for (int i = 0; i < DATA_COUNT; i++) {
			System.out.printf("num[%d] = %d; ", i, num[i]);
		}
		System.out.printf("\n");
	}
	
	
	public static void main(String args[]) {
		
		while (true) { // continuously getting data
			startServer();
			parseTemperature();
			startProducer();
		}
		
	}
}
