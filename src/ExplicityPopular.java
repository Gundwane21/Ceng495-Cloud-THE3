import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class PartitionerExample extends Configured implements Tool
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

		// put only explicit and popularity
            String exAndPop =  parsedLine[3] + '\t' + parsedLine[5] ;
	    
            Text explicitAndPopularity = new Text(exAndPop);
            Text songName = new Text(parsedLine[1]);    
            context.write(songName,explicitAndPopularity);
            }
        }catch(Exception e){
		e.printStackTrace();
			
	    }

      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
   {
      private int sum = 0;
      private int counter = 0;
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
		
         for (Text val : values)
         {
            String [] str = val.toString().split("\t");
	    int popularity = Integer.parseInt(str[1]);
            sum += popularity;
	    counter++; 
	}
			
//         context.write(new Text(key), new IntWritable(max));

      }

	    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
      {
 	 context.write(new Text("Average"), new Intwritable(sum/counter) );
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
         boolean explicit = Boolean.parseBoolean(str[0]);        
 
         if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if(explicit)
         {
            return 0 % numReduceTasks;
         }
         else
         {
            return 1 % numReduceTasks;
         }
      }
   }
   
   @Override
   public int run(String[] arg) throws Exception
   {
      Configuration conf = getConf();
		
      Job job = new Job(conf, "explicitlyPopular");
      job.setJarByClass(PartitionerExample.class);
		
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
      int res = ToolRunner.run(new Configuration(), new PartitionerExample(),ar);
      System.exit(0);
   }
}
