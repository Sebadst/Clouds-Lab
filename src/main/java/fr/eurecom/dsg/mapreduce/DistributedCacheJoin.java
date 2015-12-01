package fr.eurecom.dsg.mapreduce;

/**
 * Created by sds on 01/11/2015.
 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DistributedCacheJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputFile;
    private Path inputTinyFile;
    private int numReducers;

    public DistributedCacheJoin(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: DistributedCacheJoin <num_reducers> " +
                    "<input_tiny_file> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputTinyFile = new Path(args[1]);
        this.inputFile = new Path(args[2]);
        this.outputDir = new Path(args[3]);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // TODO: add the smallFile to the distributed cache

        DistributedCache.addCacheFile(new Path(args[2]).toUri(),conf);
        //oppure questo?
        //DistributedCache.addLocalFiles(conf,args[2]);
        Job job = new Job(conf,"DCJoin"); // TODO: define new job instead of null using conf e setting
        // a name

        job.setInputFormatClass(TextInputFormat.class);// TODO: set job input format
        job.setMapperClass(DCJoinMapper.class);// TODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(DCJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);// TODO: set job output format
        // inputFile
        FileInputFormat.addInputPath(job, new Path(args[1]));// TODO: add the input file as job input (from HDFS) to the variable
        // FileInputFormat.addInputPath(job, new Path(args[2]));

        FileOutputFormat.setOutputPath(job, new Path(args[3])); // TODO: set the output path for the job results (to HDFS) to the variable
        // outputPath
        job.setNumReduceTasks(Integer.parseInt(args[0]));// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(WordCount.class); // TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DistributedCacheJoin(args),
                args);
        System.exit(res);
    }

    // TODO: implement mapper
    static class DCJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable ONE = new IntWritable(1);
        private Text textValue = new Text();
        private Set<String> stopWordList = new HashSet<String>();
        private BufferedReader fis;

        @Override
        protected void setup(Context context) throws java.io.IOException,
                InterruptedException {

            try {

                Path[] stopWordFiles = new Path[0];
                stopWordFiles = context.getLocalCacheFiles();
                //System.out.println(stopWordFiles.toString());
                if (stopWordFiles != null && stopWordFiles.length > 0) {
                    for (Path stopWordFile : stopWordFiles) {
                        readStopWordFile(stopWordFile);
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading stop word file: " + e);

            }

        }
        private void readStopWordFile(Path stopWordFile) {
            try {
                fis = new BufferedReader(new FileReader(stopWordFile.toString()));
                String stopWord = null;
                //controllare se sono una stopword per riga oppure se devo splittare normalmente
                while ((stopWord = fis.readLine()) != null) {
                    stopWordList.add(stopWord);
                }
            } catch (IOException ioe) {
                System.err.println("Exception while reading stop word file '"
                        + stopWordFile + "' : " + ioe.toString());
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            String line = value.toString();
            String[] words = line.split("\\s+");
            for(String word : words) {
                if (!stopWordList.contains(word)) {
                    textValue.set(word);
                    context.write(textValue, ONE);
                }
            }
        }
    }



    // TODO: implement reducer
    static class DCJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable writableSum = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();

            writableSum.set(sum);
            context.write(key,writableSum);
        }
    }

}

