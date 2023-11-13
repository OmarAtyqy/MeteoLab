// This the main class. It is used to run the MapReduce job.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeteoMain {

    public static void main(String[] args) throws Exception {

        // check if the number of arguments is correct
        if (args.length != 2) {
            System.err.println("Usage: MeteoMain <input path> <output path>");
            System.exit(-1);
        }

        // get the input and output paths from the arguments
        String inputPath = args[0];
        String outputPath = args[1];

        // create a new configuration
        Configuration conf = new Configuration();

        // create a new job
        Job job = Job.getInstance(conf, "MeteoLab");

        // set the main class
        job.setJarByClass(MeteoMain.class);

        // set the mapper and reducer classes
        job.setMapperClass(MeteoMappper.class);
        job.setReducerClass(MeteoReducer.class);

        // set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set the input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}