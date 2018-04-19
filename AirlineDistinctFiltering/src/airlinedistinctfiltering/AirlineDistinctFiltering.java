/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinedistinctfiltering;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dipti
 */
public class AirlineDistinctFiltering {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Distinct Filtering");
        
        job.setJarByClass(AirlineDistinctFiltering.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class DistinctMapper extends Mapper<Object,Text,Text,NullWritable>{
        
        private Text airline = new Text();
        
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] columns = value.toString().split(",");
            airline.set(columns[1].trim());
            
            context.write(airline,NullWritable.get());
        }
    }
    
    public static class DistinctReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        public void reduce(Text key,Iterable<NullWritable>values,Context context) throws IOException, InterruptedException{
            context.write(key,NullWritable.get());
        }
    }
    
}
