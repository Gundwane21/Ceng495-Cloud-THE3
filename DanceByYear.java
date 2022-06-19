import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;
import org.apache.hadoop.io.IntWritable;

public class DanceByYear extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
      

        String line = value.toString();

        try{
            if (key.get() == 0) 
                return;
            
            else{

            String[] parsedLine = line.split("\t");

		// put only year and danceability
            String yearAndDance =  parsedLine[4] + '\t' + parsedLine[6] ;
	    
            Text yearAndDanceText = new Text(yearAndDance);
            Text songName = new Text(parsedLine[1]);    
            context.write(songName,yearAndDanceText);
            }
        }catch(Exception e){
		e.printStackTrace();
			
	    }

      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,DoubleWritable>
   {
      private double sum = 0;
      private int counter = 0;
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
		
         for (Text val : values)
         {
            String [] str = val.toString().split("\t");
            double danceability = Double.parseDouble(str[1]);
            sum += danceability;
	         counter++; 
	      }
			
      }

	    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
      {
      if (counter > 0){
         context.write(new Text("Average Danceability"), new DoubleWritable(sum/ (double) counter) );
         }
      }
   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split("\t");
         int year = Integer.parseInt(str[0]);        
 
         if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if(year <= 2002)
         {
            return 0 % numReduceTasks;
         }
         else if (year > 2002 && year <= 2012)
         {
            return 1 % numReduceTasks;
         }
         else{
            return 2 % numReduceTasks;
         }
      }
   }
   
   @Override
   public int run(String[] arg) throws Exception
   {
      Configuration conf = getConf();
		
      Job job = new Job(conf, "dancebyyear");
      job.setJarByClass(DanceByYear.class);
		
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(CaderPartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(3);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
   public static void main(String ar[]) throws Exception
   {
      int res = ToolRunner.run(new Configuration(), new DanceByYear(),ar);
      System.exit(0);
   }
}
