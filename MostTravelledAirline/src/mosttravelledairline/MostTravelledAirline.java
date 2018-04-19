/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mosttravelledairline;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dipti
 */
public class MostTravelledAirline {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Most Travelled Airline");
        
        job.setJarByClass(MostTravelledAirline.class);
        job.setMapperClass(MostTravelledAirlineMapper.class);
        job.setCombinerClass(MostTravelledAirlineReducer.class);
        job.setReducerClass(MostTravelledAirlineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        
        boolean complete = job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Most Travelled Airline");
        
        if(complete) {
        
        job2.setJarByClass(MostTravelledAirline.class);
        
        job2.setMapperClass(MostTravelledAirlineMapper2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setReducerClass(MostTravelledAirlineReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setSortComparatorClass(MTASortKeyComparator.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
        System.exit(job2.waitForCompletion(true) ?0:1);
        }
        
    }
    
    public static class MostTravelledAirlineMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        
        @Override
        public void map(LongWritable key,Text value,Context context) {
            try{
        String row[] = value.toString().split(",");
        String airline = row[0].trim();
        context.write(new Text(airline), one);
            } catch (Exception e){
                e.printStackTrace();
            }
        
        }
    }
    
    public static class MostTravelledAirlineReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
    
    @Override
    protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        try{
            int sum =0;
        for(IntWritable val:values){
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
   }
    
    public static class MostTravelledAirlineMapper2 extends Mapper<LongWritable,Text,IntWritable,Text> {
        
        @Override
        public void map(LongWritable key,Text value,Context context){
            try{
            String row[] = value.toString().split("\\t");
            
            Text airline = new Text(row[0]);
            IntWritable count = new IntWritable(Integer.parseInt(row[1].trim()));
            
            context.write(count, airline);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
    
    public static class MostTravelledAirlineReducer2 extends Reducer<IntWritable,Text,IntWritable,Text> {
        public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
            for(Text val:value) {
                context.write(key,val);
            }
        }
    }
    
}
