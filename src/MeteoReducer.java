// This is the reducer class. It is used to aggregate the values of the same key.
// The input data is a list of <key, value> pairs, where the key is a string containing the station id, region type and the date of the measurement, and the value is the temperature
// The output data is a list of <key, value> pairs, where the key is a string containing the station id, region type and the date of the measurement
// and the value is the average temperature, max, min, standard deviation and median of the temperature for that key.

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MeteoReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    // define the key and value as global variables
    private Text word = new Text();
    private Text result = new Text();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
            InterruptedException {

        // get the region type, station id and year from the key
        String[] keyParts = key.toString().split("-");
        String regionType = keyParts[0];
        String stationId = keyParts[1];
        String year = keyParts[2];

        // create a list to store the temperatures
        List<Double> temperatures = new ArrayList<>();

        // iterate over the values and add them to the list
        for (DoubleWritable value : values) {
            temperatures.add(value.get());
        }

        // get the average of the temperatures
        double average = temperatures.stream().mapToDouble(val -> val).average().orElse(0.0);

        // get the maximum of the temperatures
        double max = temperatures.stream().mapToDouble(val -> val).max().orElse(0.0);

        // get the minimum of the temperatures
        double min = temperatures.stream().mapToDouble(val -> val).min().orElse(0.0);

        // get the standard deviation of the temperatures
        double standardDeviation = 0.0;
        if (temperatures.size() > 0) {
            double sum = 0.0;
            for (double temperature : temperatures) {
                sum += Math.pow(temperature - average, 2);
            }
            standardDeviation = Math.sqrt(sum / temperatures.size());
        }

        // get the median of the temperatures
        double median = 0.0;
        if (temperatures.size() > 0) {
            Collections.sort(temperatures);
            int middle = temperatures.size() / 2;
            if (temperatures.size() % 2 == 0) {
                median = (temperatures.get(middle) + temperatures.get(middle - 1)) / 2.0;
            } else {
                median = temperatures.get(middle);
            }
        }

        // create the key and value string
        String valueString = String.format("%.2f-%.2f-%.2f-%.2f-%.2f", average, max, min, standardDeviation, median);
        String keyString = String.format("%s-%s-%s", stationId, year, regionType);

        // set the key and value
        word.set(keyString);
        result.set(valueString);

        // write the key-value pair to the context
        context.write(word, result);
    }
}
