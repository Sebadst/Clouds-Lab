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


public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "stripes");  // TODO: define new job instead of null using conf e setting a name

        job.setInputFormatClass(TextInputFormat.class);// TODO: set job input format

        job.setMapperClass(StripesMapper.class);// TODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringToIntMapWritable.class);

        job.setReducerClass(StripesReducer.class);// TODO: set reduce class and the reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringToIntMapWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);// TODO: set job output format

        FileInputFormat.addInputPath(job, this.inputPath);// TODO: add the input file as job input (from HDFS) to the variable
        FileOutputFormat.setOutputPath(job, this.outputDir);// TODO: set the output path for the job results (to HDFS) to the variable
        job.setNumReduceTasks(this.numReducers);// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(Stripes.class);// TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Stripes (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }
}

class StripesMapper
        extends Mapper<LongWritable,   // TODO: change Object to input key type
        Text,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type


    @Override
    public void map(LongWritable key, // TODO: change Object to input key type
                    Text value, // TODO: change Object to input value type
                    Context context)
            throws java.io.IOException, InterruptedException {

        // TODO: implement map method
        String line = value.toString();
        String[] words = line.split("\\s+");

        for(String word : words) {
            StringToIntMapWritable v = new StringToIntMapWritable();
            for(String word2 : words){
                if(!word.equals(word2)){
                    if(v.get(word2) == null){
                        v.set(word2, 1);
                    }
                    else{
                        v.set(word2, v.get(word2) + 1);
                    }
                }
            }
            context.write(new Text(word), v);
        }
    }
}

class StripesReducer
        extends Reducer<Text,   // TODO: change Object to input key type
        StringToIntMapWritable,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type

    @Override
    public void reduce(Text key, // TODO: change Object to input key type
                       Iterable<StringToIntMapWritable> values, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method
        // TODO: implement the reduce method
        StringToIntMapWritable h = new StringToIntMapWritable();
        for (StringToIntMapWritable value : values){
            for(String k : value.getKeySet()){
                if(h.get(k) == null){
                    h.set(k, value.get(k));
                 }
                else{
                    h.set(k, h.get(k) + value.get(k));
                }
            }
         }
        context.write(key, h);
    }

}