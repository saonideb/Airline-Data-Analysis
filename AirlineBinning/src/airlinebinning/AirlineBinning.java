/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinebinning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author dipti
 */
public class AirlineBinning {

    public static class ABMapper extends Mapper<Object, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> mos = null;
        @Override
        protected void setup(Context context) {

            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                List<String> carriers = new ArrayList<>(Arrays.asList("9E", "AA","AQ","AS","B6","CO",
                        "DH","DL","EV","F9","FL","HA","HP","MQ","NK","NW","OH","OO","RU","TZ","UA","US",
                        "VX","WN","XE","YV"));
                if (value.toString().contains("year") /*Some condition satisfying it is header*/) {
                    return;
                } 
                else {
                    String[] columns = value.toString().split(",");
                    String carrier = columns[2].trim();

                for(int i=0;i<carriers.size();i++){
                    if(carrier.equalsIgnoreCase(carriers.get(i))){
                        mos.write("bins", value, NullWritable.get(), carriers.get(i));
                        break;
                    }
                    else
                        continue;
                }


                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            mos.close();
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Binning");
        job.setJarByClass(AirlineBinning.class);

        job.setNumReduceTasks(0);

        job.setMapperClass(ABMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);

        MultipleOutputs.setCountersEnabled(job, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
