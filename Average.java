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

public class Average {

  /**
   *
   * Mapper class
   **/
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, LongWritable>{

     @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();

        try{
            if (key.get() == 0) 
                return;
            
            else{

            String[] parsedLine = line.split("\t");
            long duration = Long.parseLong(parsedLine[2] );
            LongWritable durationWritable = new LongWritable(duration);
            Text songName = new Text(parsedLine[1]);
            System.out.println(String.format("songName: %s, duration: %d ", parsedLine[1] ,duration));
                
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
  public static class LongSumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable(0);

        private long sum = 0;
	private long count = 0;
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
          for(LongWritable val: values){
            System.out.println("value"+ val.get());
            sum +=  val.get();
	    count++;	
          }
        }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {
              context.write(new Text("average"),new LongWritable(sum/count));
    }
  } 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,  "average");
    job.setJarByClass(Average.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
