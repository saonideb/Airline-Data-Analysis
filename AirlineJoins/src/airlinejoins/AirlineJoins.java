/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinejoins;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author dipti
 */
public class AirlineJoins {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "AirlineJoins");
        job1.setJarByClass(AirlineJoins.class);

        
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RouteAirportMapper1.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RouteAirportMapper2.class);
        job1.getConfiguration().set("join.type", "inner");
        
        job1.setReducerClass(RouteAirportReducer.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        System.exit(job1.waitForCompletion(true)? 0 :2);
//        boolean success = job1.waitForCompletion(true);
//            return success ? 0:2;
    }

    public static class RouteAirportMapper1 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] separatedInput = value.toString().split(",");
            String sourceAirportID = separatedInput[3].trim();
            if (sourceAirportID == null) {
                return;
            }

            outkey.set(sourceAirportID);

            outvalue.set("A" + value.toString());
            context.write(outkey, outvalue);
        }
    }

    public static class RouteAirportMapper2 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] separatedInput = value.toString().split(",");

            String airportID = separatedInput[0].trim();
            if (airportID == null) {
                return;
            }
            outkey.set(airportID);
            outvalue.set("B" + value.toString());
            context.write(outkey, outvalue);
        }
    }

    public static class RouteAirportReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text();
        private Text tmp = new Text();

        private ArrayList<Text> listA = new ArrayList<>();
        private ArrayList<Text> listB = new ArrayList<>();

        private String joinType = null;

        public void setup(Context context) {
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();
            while (values.iterator().hasNext()) {
                tmp = values.iterator().next();

                if (Character.toString((char) tmp.charAt(0)).equals("A")) {

                    listA.add(new Text(tmp.toString().substring(1)));
                }
                if (Character.toString((char) tmp.charAt(0)).equals("B")) {
                    listB.add(new Text(tmp.toString().substring(1)));
                }

            }

            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {

            if (joinType.equalsIgnoreCase("inner")) {

                if (!listA.isEmpty() && !listB.isEmpty()) {

                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            }
        }
    }
}
