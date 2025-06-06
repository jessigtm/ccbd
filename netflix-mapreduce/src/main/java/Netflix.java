import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class Netflix {

	public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            try{
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                int x = s.nextInt();
                s.nextInt();
                int y = s.nextInt();
                s.toString();
                context.write(new IntWritable(x),new IntWritable(y));
                s.close();
            }catch(Exception e){
                //Skip line
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,LongWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            long count = 0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new LongWritable(count));
        }
    }
    
    //Part 2
    public static class MyMapper2 extends Mapper<Object,Text,Text,DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            try{
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                String x = s.next();
                s.nextInt();
                Double y = s.nextDouble();
                String a = s.next();
                a = a.substring(0,4);
                x = x + "-" + a;
                context.write(new Text(x),new DoubleWritable(y));
                s.close();
            }catch(Exception e){
                //skip line
            }
        }
    }

    public static class MyReducer2 extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        @Override
        public void reduce ( Text key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (DoubleWritable v: values) {
                sum += v.get();
                count++;
            };
	    double result = sum / count;
	    //result = Math.round(result * Math.pow(10, 7)) / Math.pow(10, 7);
	    BigDecimal rbd = new BigDecimal(result);
	    rbd = rbd.setScale(7, RoundingMode.HALF_UP);
	    result = rbd.doubleValue();
            context.write(key, new DoubleWritable(result));
        }
    }
    public static void main ( String[] args ) throws Exception {
        /* put your main program here */
    	Job job = Job.getInstance();
        job.setJobName("NetflixJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    	
	//Part 2 Job
	Job job2 = Job.getInstance();
        job2.setJobName("NetflixJob2");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[0]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
	job2.waitForCompletion(true);
    }
}
