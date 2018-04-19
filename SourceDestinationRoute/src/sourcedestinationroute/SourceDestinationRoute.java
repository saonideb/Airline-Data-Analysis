/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sourcedestinationroute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dipti
 */
public class SourceDestinationRoute {

    /**
     * @param args the command line arguments
     */
    public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable>{

        private String source,destination;
        
        public CompositeKeyWritable()
    {
        
    }
    public CompositeKeyWritable(String s, String d) {
        this.source = s;
        this.destination = d;
    }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }
    
        @Override
        public void write(DataOutput d) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            WritableUtils.writeString(d, source);
        WritableUtils.writeString(d, destination);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
           // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
           source = WritableUtils.readString(di);
        destination = WritableUtils.readString(di);
        }

        @Override
    public int compareTo(CompositeKeyWritable o) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        int result = source.compareTo(o.source);
        if(result == 0) {
            result = destination.compareTo(o.destination);
        }
        return result;
    }
    @Override
    public String toString() {
        return (new StringBuilder().append(source).append("\t").append(destination).toString());
    }
        
}
    public static class GroupComparatoor extends WritableComparator {
        protected GroupComparatoor()
    {
        super(CompositeKeyWritable.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        CompositeKeyWritable cw1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable cw2 = (CompositeKeyWritable) w2;
        
        return (cw1.getSource().compareTo(cw2.getSource()));
    }
    }
    
    public static class SourceDestinationWritable implements Writable{

        private String stops,airline;

        

        public String getStops() {
            return stops;
        }

        public void setStops(String stops) {
            this.stops = stops;
        }

        public String getAirline() {
            return airline;
        }

        public void setAirline(String airline) {
            this.airline = airline;
        }
        
        
        @Override
        public void write(DataOutput out) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//        WritableUtils.writeString(out, sourceId);
//        WritableUtils.writeString(out, destination);
//        WritableUtils.writeString(out, destinationId);
        WritableUtils.writeString(out, stops);
        WritableUtils.writeString(out, airline);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//        sourceId = WritableUtils.readString(di);
//        destination = WritableUtils.readString(di);
//        destinationId = WritableUtils.readString(di);
        stops = WritableUtils.readString(di);
        airline = WritableUtils.readString(di);
        }
        
        @Override
        public String toString() {
            
            return (new StringBuilder().append(stops).append("\t").append(airline).toString());
                    //append("\t").
                    //append(destinationId).append("\t").append(stops).append("\t").append(airline).
                    //toString());
        }
        
    }
    
    public static class SDPartitioner extends Partitioner<CompositeKeyWritable,SourceDestinationWritable>{

        @Override
        public int getPartition(CompositeKeyWritable key, SourceDestinationWritable value, int numOfPartitions) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return (key.getSource().hashCode() % numOfPartitions);
        
        }
        
    }
    
    public static class SourceDestinationMapper extends Mapper<LongWritable, Text, CompositeKeyWritable,SourceDestinationWritable>{
        
        private SourceDestinationWritable sdw = new SourceDestinationWritable();
        CompositeKeyWritable cw = new CompositeKeyWritable();
        //private Text source = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
     {
      
         try{
             String[] row = value.toString().split(",") ;
             
             String sid=row[3].trim();
             String dest = row[4].trim();
             String destid = row[5].trim();
             String stops = row[7].trim();
             String airline = row[0].trim();
             
             cw.setSource(row[2].trim());
             cw.setDestination(dest);
             
//             sdw.setSourceId(sid);
//             sdw.setDestination(dest);
//             sdw.setDestinationId(destid);
             sdw.setStops(stops);
             sdw.setAirline(airline);
             
             context.write(cw, sdw);
             
         }catch(Exception e){
             e.printStackTrace();
         }
    }
    }
    
    public static class SourceDestinationReducer extends Reducer<CompositeKeyWritable,SourceDestinationWritable,CompositeKeyWritable,SourceDestinationWritable>{
        
        private  SourceDestinationWritable sdw = new SourceDestinationWritable();
        public void reduce(CompositeKeyWritable key, Iterable<SourceDestinationWritable> values, Context context) throws IOException, InterruptedException
  {
      for(SourceDestinationWritable val:values){
//          sdw.setSourceId(val.getSourceId());
//          sdw.setDestination(val.getDestination());
//          sdw.setDestinationId(val.getDestinationId());
          sdw.setStops(val.getStops());
          sdw.setAirline(val.getAirline());
      }
      context.write(key, sdw);
  }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Source Destination Route");
        
        job.setJarByClass(SourceDestinationRoute.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(SourceDestinationMapper.class);
      //job.setGroupingComparatorClass(GroupComparatoor.class);
      job.setReducerClass(SourceDestinationReducer.class);


      job.setOutputKeyClass(CompositeKeyWritable.class);
      job.setOutputValueClass(SourceDestinationWritable.class);

      job.setMapOutputKeyClass(CompositeKeyWritable.class);
      job.setMapOutputValueClass(SourceDestinationWritable.class);

      //job.setPartitionerClass(SDPartitioner.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
