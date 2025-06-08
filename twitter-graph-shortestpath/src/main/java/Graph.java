import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Integer> following;  // the vertex neighbors

    Tagged () { tag = false; }
    Tagged ( int d ) { tag = false; distance = d; following = new Vector<Integer>(); }
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        out.writeInt(following.size());
        for(Integer follower : following){
            out.writeInt(follower);
        }
    } 
    
    public void readFields(DataInput in) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        int size = in.readInt();
        following = new Vector<>();
        for(int i=0;i<size;i++){
            following.add(in.readInt());
        }
    }
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    /* ... */
    public static class Mapper1 extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int id = s.nextInt();
            int follower = s.nextInt();

            context.write(new IntWritable(follower),new IntWritable(id));
            s.close();
        }
    }

    public static class Reducer1 extends Reducer<IntWritable,IntWritable,IntWritable,Tagged> {
        @Override
        public void reduce ( IntWritable follower, Iterable<IntWritable> ids, Context context ) throws IOException, InterruptedException {
            Vector<Integer> following = new Vector<Integer>();
            for(IntWritable id : ids){
                following.add(id.get());
            }
            if(follower.get()==start_id || follower.get() == 1){
                context.write(follower, new Tagged(0, following));
            }
            else{
                context.write(follower, new Tagged(max_int, following));
            }
        }
    }

    public static class Mapper2 extends Mapper<IntWritable,Tagged,IntWritable,Tagged> {
        @Override
        public void map ( IntWritable key, Tagged value, Context context ) throws IOException, InterruptedException {            
            context.write(key,value);
            if(value.distance < max_int){
                for(Integer id: value.following){
                    context.write(new IntWritable(id), new Tagged(value.distance + 1));
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<IntWritable,Tagged,IntWritable,Tagged> {
        @Override
        public void reduce ( IntWritable id, Iterable<Tagged> values, Context context ) throws IOException, InterruptedException {
            int m = max_int;
            Vector<Integer> following = new Vector<Integer>();
            for(Tagged v: values){
                if(v.distance < m){
                    m = v.distance;
                }
                if(v.tag){
                    following = new Vector<Integer>(v.following);
                }
            }
            context.write(id, new Tagged(m, following));
        }
    }

    public static class Mapper3 extends Mapper<IntWritable,Tagged,IntWritable,IntWritable> {
        @Override
        public void map ( IntWritable key, Tagged value, Context context ) throws IOException, InterruptedException {
            if(value.distance < max_int){
                context.write(key, new IntWritable(value.distance));
            }
        }
    }

    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job job1 = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        job1.setJobName("Job1");
        job1.setJarByClass(Graph.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Tagged.class);

        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        /* ... */
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"0"));
        job1.waitForCompletion(true);
        for ( short i = 0; i < iterations; i++ ) {
            Job job2 = Job.getInstance();
            /* ... Second Map-Reduce job to calculate shortest distance */
            job2.setJobName("Job2");
            job2.setJarByClass(Graph.class);

            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Tagged.class);

            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Tagged.class);

            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            /* ... */
            FileInputFormat.setInputPaths(job2,new Path(args[1]+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        /* ... Last Map-Reduce job to output the distances */
        job3.setJobName("Job3");
        job3.setJarByClass(Graph.class);

        
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);

        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(IntWritable.class);


        job3.setMapperClass(Mapper3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        /* ... */
        FileInputFormat.setInputPaths(job3,new Path(args[1]+iterations));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}
