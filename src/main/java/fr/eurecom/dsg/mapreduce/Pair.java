package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

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


public class Pair extends Configured implements Tool {

    public static class PairMapper
            extends Mapper<LongWritable, // TODO: change Object to input key type
            Text, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            IntWritable> { // TODO: change Object to output value type
        // TODO: implement mapper

        private IntWritable ONE = new IntWritable(1);
        private TextPair textValue = new TextPair();

        @Override
        protected void map(LongWritable key, // TODO: change Object to input key type
                           Text value, // TODO: change Object to input value type
                           Context context) throws IOException, InterruptedException {

            // TODO: implement the map method (use context.write to emit results)

            String line = value.toString();
            String[] words = line.split("\\s+");
            for(String word : words) {
                for(String word2 : words){
                    if(!word.equals(word2)){
                        textValue.set(new Text(word), new Text(word2));
                        context.write(textValue,ONE);
                    }
                }
            }
        }
    }

    public static class PairReducer
            extends Reducer<TextPair, // TODO: change Object to input key type
            IntWritable, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            IntWritable> { // TODO: change Object to output value type
        // TODO: implement reducer

        private IntWritable writableSum = new IntWritable();

        @Override
        protected void reduce(TextPair key, // TODO: change Object to input key type
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

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "pairs"); // TODO: define new job instead of null using conf e setting a name

        job.setInputFormatClass(TextInputFormat.class);// TODO: set job input format
        job.setMapperClass(PairMapper.class);// TODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(PairReducer.class);// TODO: set reduce class and the reduce output key and value classes
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);// TODO: set job output format

        FileInputFormat.addInputPath(job, this.inputPath);// TODO: add the input file as job input (from HDFS) to the variable
        FileOutputFormat.setOutputPath(job, this.outputDir);// TODO: set the output path for the job results (to HDFS) to the variable
        job.setNumReduceTasks(this.numReducers);// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(Pair.class);// TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }
}
