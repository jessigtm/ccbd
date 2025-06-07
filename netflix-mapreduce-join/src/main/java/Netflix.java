import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class RatingTitle implements Writable {
    public short tag;
    public int rating;
    public String title;

    /* put your code here */
    RatingTitle() {}

    RatingTitle(int rating){
        tag = 0;
        this.rating = rating;
    }

    RatingTitle(String title) {
        tag = 1;
        this.title = title;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        if(tag==0){
            out.writeInt(rating);
        }
        else{
            out.writeUTF(title);
        }
    } 
    
    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        if(tag==0){
            rating = in.readInt();
        }
        else {
            title = in.readUTF();
        }
    }
}

public class Netflix {
    public static class RatingMapper extends Mapper<Object, Text, IntWritable, RatingTitle>{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int id = s.nextInt();
            s.nextInt();
            RatingTitle r = new RatingTitle(s.nextInt());
            s.toString();
            context.write(new IntWritable(id), r);
            s.close();
        }catch(Exception e){
            return;
        }

        }
    }

        public static class TitleMapper extends Mapper<Object, Text, IntWritable, RatingTitle>{
            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                try{
                    Scanner s = new Scanner(value.toString()).useDelimiter(",");
                    int id = s.nextInt();
                    String year = s.next();
                    String title1 = s.nextLine();
                    while (title1.startsWith(",")) {
                        title1 = title1.substring(1);
                    }
                    String title = year+": "+title1;
                    RatingTitle t = new RatingTitle(title);
                    context.write(new IntWritable(id), t);
                    s.close();
                }catch(Exception e){
                    return;
                }
            }
        }
        
    public static class ResultReducer extends Reducer<IntWritable, RatingTitle, Text, FloatWritable>{
        static Vector<RatingTitle> rating = new Vector<RatingTitle>();
        static Vector<RatingTitle> titles = new Vector<RatingTitle>();
        @Override
        public void reduce(IntWritable key, Iterable<RatingTitle> values, Context context) throws IOException, InterruptedException{
            rating.clear();
            titles.clear();
            float sum = 0.0f;
            long count = 0;
            for(RatingTitle v: values)
            {
                if(v.tag == 0){
                    sum += v.rating;
                    count++;
                }
                else{
                    titles.add(v);
                }
            }
            if(count == 0 || titles.isEmpty()){
                return;
            }
            float result = (float) (sum/count);
            for(RatingTitle title : titles){
                context.write(new Text(title.title), new FloatWritable(result));
            }
        }
    } 
    

    public static void main ( String[] args ) throws Exception {
        /* put your code here */
        Job job = Job.getInstance();
        job.setJobName("NetflixJoinJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(RatingTitle.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RatingTitle.class);
        job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,RatingMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TitleMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
