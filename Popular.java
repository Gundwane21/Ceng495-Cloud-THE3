import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Popular {

  /**
   *
   * Mapper class
   **/
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

      private IntWritable one = new IntWritable(1);
     @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();

        try{
            if (key.get() == 0) 
                return;
            
            else{

            String[] parsedLine = line.split("\t");
            Text artist = new Text(parsedLine[0]);
                
            context.write(artist,one);
            }
        }catch(Exception e){
		e.printStackTrace();
			
	    }
    }
  }

  /**
   *Reducer class
   *
   **/
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,LongWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
          long sum=0;
          for(IntWritable val: values){
            sum +=  val.get();
         
          }
          context.write(key,new LongWritable(sum));
        }
  } 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,  "popular");
    job.setJarByClass(Popular.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
