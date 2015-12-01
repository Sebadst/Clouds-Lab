package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCountIMC extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "Word Count"); // TODO: define new job instead of null using conf

        job.setInputFormatClass(TextInputFormat.class);// TODO: set job input format

        job.setMapperClass(WCIMCMapper.class);// TODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WCIMCReducer.class);// TODO: set reduce class and the reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);// TODO: set job output format

        FileInputFormat.addInputPath(job, this.inputPath);// TODO: add the input file as job input (from HDFS) to the variable
        //       inputPath
        FileOutputFormat.setOutputPath(job, this.outputDir);// TODO: set the output path for the job results (to HDFS) to the variable
        //       outputPath
        job.setNumReduceTasks(this.numReducers);// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(WordCountIMC.class);// TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCountIMC (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
        System.exit(res);
    }
}

class WCIMCMapper extends Mapper<LongWritable, // TODO: change Object to input key type
        Text, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type
    private IntWritable ONE = new IntWritable(1);
    private Text textValue = new Text();
    private HashMap<String, Integer> H;


    protected void setup(Context context) {
        this.H = new HashMap<String, Integer>();
    }

    @Override
    protected void map(LongWritable key, // TODO: change Object to input key type
                       Text value, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // * TODO: implement the map method (use context.write to emit results). Use
        // the in-memory combiner technique
        String line = value.toString();
        String[] words = line.split("\\s+");

        for(String word : words) {
            //textValue.set(word);
            Integer val = H.get(word);
            if (val == null)
                H.put(word, new Integer(1));
            else
                H.put(word, val+1);
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String t : H.keySet())
            context.write(new Text(t) ,new IntWritable(H.get(t)));
    }
}

class WCIMCReducer extends Reducer<Text, // TODO: change Object to input key type
        IntWritable, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private IntWritable writableSum = new IntWritable();

    @Override
    protected void reduce(Text key, // TODO: change Object to input key type
                          Iterable<IntWritable> values, // TODO: change Object to input value type
                          Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method (use context.write to emit results)
        int sum = 0;
        for (IntWritable value : values)
            sum += value.get();

        writableSum.set(sum);
        context.write(key,writableSum);
    }
}
