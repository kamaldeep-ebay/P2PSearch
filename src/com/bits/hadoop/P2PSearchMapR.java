package com.bits.hadoop;
 	
 	import java.io.IOException;
 	import java.util.*;
 	
 	import org.apache.hadoop.fs.Path;
 	
 	import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
 	public class P2PSearchMapR {
 	
 	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 	     
 		   String keyname ;
 		   Text outputkey; 		   
 		  @Override
 		    public void configure(JobConf conf)
 		    {
 		        // TODO Auto-generated method stub
 		        super.configure(conf);
 		        keyname = new String();
 		        keyname = conf.get("key");
 		        
 		    }
 		  
 	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
 	      
 	       
 	       /*StringTokenizer tokenizer = new StringTokenizer(line);
 	       
 	       while (tokenizer.hasMoreTokens()) {
 	       
 	    	 word.set(tokenizer.nextToken());
 	         output.collect(word, one);
 	       }*/
 	    	 String line = value.toString();
	    	 String[] array = line.split(",");
	    	 
	    	 outputkey = new Text();
	    	 
	    	 if(keyname.equalsIgnoreCase(array[0].trim()))
	    	 {
	    		 outputkey.set(array[0].trim());
	    	 	    	    	 
	    	     output.collect(outputkey, new Text(array[1].trim()));
	    	 }
	      }
 	   }
 	
 	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
 	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
 	       //int sum = 0;
 	       StringBuilder ips = new StringBuilder();
 	       while (values.hasNext()) {
 	        
 	    	   ips = ips.append("  ").append("$$").append("  ")
 	    			  .append(values.next().toString());
 	       }
 	       output.collect(key,new Text(ips.toString()));
 	     }
 	   }
 	
 	   public static void main(String[] args) throws Exception {
 	
 		 String key = args[2];  
 		 		 
 		 JobConf conf = new JobConf(P2PSearchMapR.class);
 	     conf.setJobName("search");
 	     
 	     conf.set("key", key);
 	     
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(Text.class);
 	
 	     conf.setMapperClass(Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
 	   }
 	}