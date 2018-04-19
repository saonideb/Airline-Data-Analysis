/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airportbloomfiltering;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.Key;

/**
 *
 * @author dipti
 */
public class AirportBloomFiltering {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        Configuration conf = new Configuration();
        //GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        //String[] otherArgs = parser.getRemainingArgs();
//        if (args.length != 2) {
//            System.err
//                    .println("Usage: BloomFilter <bloom_filter_file> <in> <out>");
//            ToolRunner.printGenericCommandUsage(System.err);
//            System.exit(2);
//        }

        DistributedCache.addCacheFile(new URI("/user/hadoop/Airline/BloomFilter.csv"), conf);
        Job job = new Job(conf, "Bloom Filter");
        job.setJarByClass(AirportBloomFiltering.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

    }

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
        //
        //private BloomFilter<String> filter = new BloomFilter.create(bloomValues,500,0.1);

        Funnel<Airport> a = new Funnel<Airport>() {

            @Override
            public void funnel(Airport airport, Sink into) {
                into.putString(airport.airportCode, Charsets.UTF_8);
            }
        };

        private BloomFilter<Airport> airports = BloomFilter.create(a, 500, 0.1);
        private URI[] files;
        private ArrayList<String> bloomValues = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
//            Path[] files = DistributedCache.getLocalCacheFiles(context
//                    .getConfiguration());
//            System.out.println("Reading Bloom filter from: " + files[0]);
//
//            DataInputStream stream = new DataInputStream(new FileInputStream(
//                    files[0].toString()));
//            filter.readFields(stream);
//            stream.close();

//start here
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            Path path = new Path(files[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = br.readLine()) != null) {

                String splits[] = line.split(",");

                bloomValues.add(splits[0]);

            }
for (String  value : bloomValues) {
                Airport a = new Airport(value);
                airports.put(a);
            }

            br.close();

            in.close();
//            Airport a1 = new Airport("IAH");
//            Airport a2 = new Airport("LHR");
//            Airport a3 = new Airport("CDG");
//            ArrayList<Airport> testairports = new ArrayList<Airport>();
//
//            testairports.add(a1);
//            testairports.add(a2);
//            testairports.add(a3);
//
//            for (Airport pr : testairports) {
//                airports.put(pr);
//            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] columns = value.toString().split(",");
                String airportIATA = columns[4].trim();
                if (airportIATA.equals("\\N") || airportIATA.equals("")) {
                    return;
                }
                Airport a = new Airport(airportIATA);

                if (airports.mightContain(a)) {
                    context.write(value, NullWritable.get());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
