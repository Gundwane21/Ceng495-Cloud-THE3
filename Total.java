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

public class Total {

  /**
   *
   * Mapper class
   **/
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, DoubleWritable>{

     @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();

        try{
            if (key.get() == 0) 
                return;
            
            else{

            String[] parsedLine = line.split(",");
            System.out.println(parsedLine);
            // if there is no entry bigger than 2^32 use int instead
            double duration = Double.parseDouble(parsedLine[2]);
            DoubleWritable durationWritable = new DoubleWritable(duration);
            Text songName = new Text(parsedLine[1]);
            System.out.println(String.format("songName: %s, duration: %f ", parsedLine[1] ,duration));
                
            context.write(songName,durationWritable);
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
  public static class DoubleSumReducer
       extends Reducer<Text,DoubleWritable,Text,IntWritable> {
        private DoubleWritable result = new DoubleWritable(0);

        public void reduce(Text key, DoubleWritable values,
                           Context context
                           ) throws IOException, InterruptedException {

            int sum = 0;
         result.set(sum);
          context.write(key, result);
        }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,  "Total");
    job.setJarByClass(Total.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(DoubleSumReducer.class);
//    job.setReducerClass(DoubleSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(DoubleWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
