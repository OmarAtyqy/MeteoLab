// This file implements the MeteoMappper class. It is used to generate <key, value> pairs from the input data.
// The input data is a file containing the weather measurements of a number of a specific station over the duration of a year.
// The output data is a list of <key, value> pairs, where the key is a string containing the station id, the region type and the date of the measurement, and the value is the temperature

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MeteoMappper extends Mapper<Object, Text, Text, DoubleWritable> {

    // We define the key and value as global variables
    private Text word = new Text();
    private DoubleWritable temperatureWritable = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Get the line from the input file
        String line = value.toString();

        // Get the information from the line
        String stationId = line.substring(4, 10).trim();
        String year = line.substring(15, 19).trim(); // only extract the year
        String temperatureString = line.substring(88, 93).trim();

        // we also get the name of the file to extract the type of region from it
        String fileName = ((FileSplit) context.getInputSplit()).getPath()
                .getName();
        String regionType = fileName.split("-")[0];

        // Check if the temperature is valid
        int i = 0;
        if (!temperatureString.equals("99999")) {

            // create the key and set it
            String compositeKey = String.format("%s-%s-%s", regionType, stationId, year);
            word.set(compositeKey);

            // create the value and set it (divide by 100 to get the temperature in degrees
            // Celsius)
            double temperature = Double.parseDouble(temperatureString);
            temperatureWritable.set(temperature / 100);

            // write the key-value pair to the context
            context.write(word, temperatureWritable);
        } else {
            i++;
            System.out.println("Invalid temperatures skipped: " + i);
        }
    }
}
