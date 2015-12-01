package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class StripesNew extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "StripesNew");

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WCOMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringToIntMapWritableNew.class);
        job.setReducerClass(WCOReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringToIntMapWritableNew.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);

        FileOutputFormat.setOutputPath(job, outputDir);

        job.setNumReduceTasks(numReducers);
        job.setJarByClass(StripesNew.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job


    }

    public StripesNew(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: StripesNew <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StripesNew(args), args);
        System.exit(res);
    }
}

class WCOMapper extends Mapper<LongWritable,
        Text,
        Text,
        StringToIntMapWritableNew> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words = line.split("\\s+");
        for (int i = 0; i < words.length ; i++){
            StringToIntMapWritableNew assArray = new StringToIntMapWritableNew();
            //We are going to skip the same word
            for(int j = 0; j < words.length; j++)
                if(!words[i].equals(words[j]))
                    assArray.add(words[j]);

            text.set(words[i]);
            context.write(text, assArray);
        }
    }
}

class WCOReducer extends Reducer<Text,
        StringToIntMapWritableNew,
        Text,
        StringToIntMapWritableNew> {

    @Override
    protected void reduce(Text key,
                          Iterable<StringToIntMapWritableNew> values,
                          Context context) throws IOException, InterruptedException {

        StringToIntMapWritableNew assArray = new StringToIntMapWritableNew();

        for (StringToIntMapWritableNew value : values)
            assArray.add(value);

        context.write(key, assArray);
    }
}
