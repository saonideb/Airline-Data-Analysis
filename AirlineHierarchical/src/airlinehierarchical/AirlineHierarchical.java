/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinehierarchical;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *
 * @author dipti
 */
public class AirlineHierarchical {

    public static class AirlineWritable implements Writable {

        private String airlineCountry, airlineName;

        public String getAirlineCountry() {
            return airlineCountry;
        }

        public void setAirlineCountry(String airlineCountry) {
            this.airlineCountry = airlineCountry;
        }

        public String getAirlineName() {
            return airlineName;
        }

        public void setAirlineName(String airlineName) {
            this.airlineName = airlineName;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            WritableUtils.writeString(d, airlineCountry);
            WritableUtils.writeString(d, airlineName);

        }

        @Override
        public void readFields(DataInput di) throws IOException {
            airlineCountry = WritableUtils.readString(di);
            airlineName = WritableUtils.readString(di);

        }

    }

    public static class AirlineHierarchicalMapper extends Mapper<Object, Text, Text, AirlineWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String column[] = value.toString().split(",");
                AirlineWritable ah = new AirlineWritable();

                Text airlineId = new Text(column[0].trim());

                if(column[6].trim().equals("\\N") || column[6].trim().equals("")){
                    return;
                }
                
                ah.setAirlineCountry(column[6].trim());
                ah.setAirlineName(column[1].trim());

                context.write(airlineId, ah);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AirlineHierarchicalReducer extends Reducer<Text, AirlineWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<AirlineWritable> values, Context context) throws IOException, InterruptedException {

            List<String> airlineNames = new ArrayList<>();
            String country = null;

            for (AirlineWritable value : values) {
                country = value.getAirlineCountry();
                airlineNames.add(value.getAirlineName());
            }
            if (!airlineNames.isEmpty() && country != null) {
                String output = writeXmlFile(country, airlineNames);
                context.write(new Text(output), NullWritable.get());
            }
        }

        private String transformDocumentToString(Document doc) throws TransformerException {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));

            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        }

        private String writeXmlFile(String country, List<String> list) {

            try {

                DocumentBuilderFactory dFact = DocumentBuilderFactory.newInstance();
                DocumentBuilder build = dFact.newDocumentBuilder();
                Document doc = build.newDocument();

                Element root = doc.createElement("airlines");
                doc.appendChild(root);

                Element countryElement = doc.createElement("country");
                countryElement.appendChild(doc.createTextNode(country));
                root.appendChild(countryElement);

                Element airlinename = doc.createElement("airline_names");

                if (!list.isEmpty()) {
                    for (String dtl : list) {
                        Element tag = doc.createElement("airline_name");
                        tag.appendChild(doc.createTextNode(dtl));
                        airlinename.appendChild(tag);
                    }
                    root.appendChild(airlinename);
                }

                // Save the document to the disk file
                TransformerFactory tranFactory = TransformerFactory.newInstance();
                Transformer aTransformer = tranFactory.newTransformer();

                // format the XML nicely
                aTransformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");

                aTransformer.setOutputProperty(
                        "{http://xml.apache.org/xslt}indent-amount", "4");
                aTransformer.setOutputProperty(OutputKeys.INDENT, "yes");

                // location and tags of XML file you can change as per need
                String output = transformDocumentToString(doc);
                return output;

            } catch (TransformerException ex) {
                System.out.println("Error outputting document");

            } catch (ParserConfigurationException ex) {
                System.out.println("Error building document");
            }
            return null;
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AirlineHierarchical");
        job.setJarByClass(AirlineHierarchical.class);
        job.setMapperClass(AirlineHierarchicalMapper.class);
        job.setReducerClass(AirlineHierarchicalReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AirlineWritable.class);
        
        //MultipleOutputs.setCountersEnabled(job, true);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
