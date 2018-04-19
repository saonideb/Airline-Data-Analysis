/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dealybyyearpartioning;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dipti
 */
public class DealyByYearPartioning {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Partitioning");
        job.setJarByClass(DealyByYearPartioning.class);

        job.setNumReduceTasks(15);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(DelayPMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(DelayPReducer.class);
        job.setPartitionerClass(DelayPPartitioner.class);
        DelayPPartitioner.setMinLastAccessDate(job,2003);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class DelayPMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outkey = new IntWritable();

        protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            try {
                String[] columns = value.toString().split(",");

                String year = columns[0].trim();
                outkey.set(Integer.parseInt(year));
                context.write(outkey, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class DelayPPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

        private Configuration conf = null;
        private int minLastAccessDateYear = 0;

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get() - minLastAccessDateYear;
        }

        public Configuration getConf() {
            return conf;
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
            minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
        }

        public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
        }
    }
    
    public static class DelayPReducer extends Reducer<IntWritable,Text,Text,NullWritable>{
        
        protected void reduce(IntWritable key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
            for(Text t:values){
                context.write(t, NullWritable.get());
            }
        }
    }
}
