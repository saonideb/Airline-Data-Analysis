/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinedelayreason;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class AirlineDelayReason {

    /**
     * @param args the command line arguments
     */
    public static class ADRWritable implements Writable {

        private float arrivalDelayAvg, carrierDelayAvg, weatherDelayAvg, nasDelayAvg,securityDelayAvg, lateAircraftAvg;
        private float arivalDelayCount, carrierDelayCount, weatherDelayCount, nasDelayCount,securityDelayCount, lateAircraftCount;

        public float getWeatherDelayAvg() {
            return weatherDelayAvg;
        }

        public void setWeatherDelayAvg(float weatherDelayAvg) {
            this.weatherDelayAvg = weatherDelayAvg;
        }

        public float getNasDelayAvg() {
            return nasDelayAvg;
        }

        public void setNasDelayAvg(float nasDelayAvg) {
            this.nasDelayAvg = nasDelayAvg;
        }

        public float getSecurityDelayAvg() {
            return securityDelayAvg;
        }

        public void setSecurityDelayAvg(float securityDelayAvg) {
            this.securityDelayAvg = securityDelayAvg;
        }

        public float getLateAircraftAvg() {
            return lateAircraftAvg;
        }

        public void setLateAircraftAvg(float lateAircraftAvg) {
            this.lateAircraftAvg = lateAircraftAvg;
        }

        public float getWeatherDelayCount() {
            return weatherDelayCount;
        }

        public void setWeatherDelayCount(float weatherDelayCount) {
            this.weatherDelayCount = weatherDelayCount;
        }

        public float getNasDelayCount() {
            return nasDelayCount;
        }

        public void setNasDelayCount(float nasDelayCount) {
            this.nasDelayCount = nasDelayCount;
        }

        public float getSecurityDelayCount() {
            return securityDelayCount;
        }

        public void setSecurityDelayCount(float securityDelayCount) {
            this.securityDelayCount = securityDelayCount;
        }

        public float getLateAircraftCount() {
            return lateAircraftCount;
        }

        public void setLateAircraftCount(float lateAircraftCount) {
            this.lateAircraftCount = lateAircraftCount;
        }

        public float getArrivalDelayAvg() {
            return arrivalDelayAvg;
        }

        public void setArrivalDelayAvg(float arrivalDelayAvg) {
            this.arrivalDelayAvg = arrivalDelayAvg;
        }

        public float getCarrierDelayAvg() {
            return carrierDelayAvg;
        }

        public void setCarrierDelayAvg(float carrierDelayAvg) {
            this.carrierDelayAvg = carrierDelayAvg;
        }

        public float getArivalDelayCount() {
            return arivalDelayCount;
        }

        public void setArivalDelayCount(float arivalDelayCount) {
            this.arivalDelayCount = arivalDelayCount;
        }

        public float getCarrierDelayCount() {
            return carrierDelayCount;
        }

        public void setCarrierDelayCount(float carrierDelayCount) {
            this.carrierDelayCount = carrierDelayCount;
        }

        @Override
        public void write(DataOutput d) throws IOException {

            d.writeFloat(arivalDelayCount);
            d.writeFloat(arrivalDelayAvg);
            d.writeFloat(carrierDelayCount);
            d.writeFloat(carrierDelayAvg);
            d.writeFloat(weatherDelayCount);
            d.writeFloat(weatherDelayAvg);
            d.writeFloat(nasDelayCount);
            d.writeFloat(nasDelayAvg);
            d.writeFloat(securityDelayCount);
            d.writeFloat(securityDelayAvg);
            d.writeFloat(lateAircraftCount);
            d.writeFloat(lateAircraftAvg);

        }

        @Override
        public void readFields(DataInput di) throws IOException {

            arivalDelayCount = di.readFloat();
            arrivalDelayAvg = di.readFloat();
            carrierDelayCount = di.readFloat();
            carrierDelayAvg = di.readFloat();
            weatherDelayCount = di.readFloat();
            weatherDelayAvg = di.readFloat();
            nasDelayCount = di.readFloat();
            nasDelayAvg = di.readFloat();
            securityDelayCount = di.readFloat();
            securityDelayAvg = di.readFloat();
            lateAircraftCount = di.readFloat();
            lateAircraftAvg = di.readFloat();
        }

        @Override
        public String toString() {
            return (new StringBuilder().append(arrivalDelayAvg).append("\t").append(carrierDelayAvg)
                    .append("\t").append(weatherDelayAvg).append("\t").append(nasDelayAvg).
                    append("\t").append(securityDelayAvg).append("\t").append(lateAircraftAvg).toString());
        }
    }

    public static class ADRMapper extends Mapper<Object, Text, Text, ADRWritable> {

        private ADRWritable adr = new ADRWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String row[] = value.toString().split(",");

                String year = row[0].trim();
                String carrier = row[2].trim();

                float arrivalDelay = Float.parseFloat(row[16].trim());
                float carrierDelay = Float.parseFloat(row[17].trim());
                float weatherDelay = Float.parseFloat(row[18].trim());
                float nasDelay = Float.parseFloat(row[19].trim());
                float securityDelay = Float.parseFloat(row[20].trim());
                float lateAircraftDelay = Float.parseFloat(row[21].trim());
                
                adr.setArivalDelayCount(1);
                adr.setCarrierDelayCount(1);
                adr.setWeatherDelayCount(1);
                adr.setNasDelayCount(1);
                adr.setSecurityDelayCount(1);
                adr.setLateAircraftCount(1);
                
                adr.setArrivalDelayAvg(arrivalDelay);
                adr.setCarrierDelayAvg(carrierDelay);
                adr.setWeatherDelayAvg(weatherDelay);
                adr.setNasDelayAvg(nasDelay);
                adr.setSecurityDelayAvg(securityDelay);
                adr.setLateAircraftAvg(lateAircraftDelay);

                context.write(new Text(new StringBuilder().append(year).append("\t").append(carrier).toString()), adr);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class ADRReducer extends Reducer<Text, ADRWritable, Text, ADRWritable> {
        
        private ADRWritable adrResult = new ADRWritable();
        
        float adCount = 0, cdCount = 0, wdCount =0, nasCount = 0, securityCount=0,lateAircraftCount=0;
        float adSum = 0 , cdSum = 0, wdSum =0, nasSum =0, securitySum=0,lateAircraftSum=0;
        
        @Override
        public void reduce(Text key, Iterable<ADRWritable> values, Context context) throws IOException, InterruptedException {
            
            for (ADRWritable val : values) {
                adSum += val.getArivalDelayCount() * val.getArrivalDelayAvg();
                adCount += val.getArivalDelayCount();
                
                cdSum += val.getCarrierDelayCount()* val.getCarrierDelayAvg();
                cdCount += val.getCarrierDelayCount();
                
                wdSum += val.getWeatherDelayCount()* val.getWeatherDelayAvg();
                wdCount += val.getWeatherDelayCount();
                
                nasSum += val.getNasDelayCount()* val.getNasDelayAvg();
                nasCount += val.getNasDelayCount();
                
                securitySum += val.getSecurityDelayCount()* val.getSecurityDelayAvg();
                securityCount += val.getSecurityDelayCount();
                
                lateAircraftSum += val.getLateAircraftCount()* val.getLateAircraftAvg();
                lateAircraftCount += val.getLateAircraftCount();
            }
            
            adrResult.setArivalDelayCount(adCount);
            adrResult.setArrivalDelayAvg(adSum / adCount);
            
            adrResult.setCarrierDelayCount(cdCount);
            adrResult.setCarrierDelayAvg(cdSum / cdCount);
            
            adrResult.setWeatherDelayCount(wdCount);
            adrResult.setWeatherDelayAvg(wdSum / wdCount);
            
            adrResult.setNasDelayCount(nasCount);
            adrResult.setNasDelayAvg(nasSum / nasCount);
            
            adrResult.setSecurityDelayCount(securityCount);
            adrResult.setSecurityDelayAvg(securitySum / securityCount);
            
            adrResult.setLateAircraftCount(lateAircraftCount);
            adrResult.setLateAircraftAvg(lateAircraftSum / lateAircraftCount);
            
            context.write(key,adrResult);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Airline Delay Reason");
        
        job.setJarByClass(AirlineDelayReason.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ADRMapper.class);
        job.setCombinerClass(ADRReducer.class);
        job.setReducerClass(ADRReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ADRWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ADRWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
