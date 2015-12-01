package fr.eurecom.dsg.mapreduce;

/**
 * Created by campo on 11/2/2015.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    static class RS1JoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {

        @Override
        public void map(LongWritable key, // TODO: change Object to input key type
                        Text value, // TODO: change Object to input value type
                        Context context)
                throws java.io.IOException, InterruptedException {

            // TODO: implement map method
            String line = value.toString();
            String[] words = line.split("\\s+");

            context.write(new TextPair(words[1], "0"), new Text(words[0]));
        }
    }

    public static class PartitionerRSJ extends
            Partitioner<TextPair, IntWritable> {
        @Override
        public int getPartition(TextPair key, IntWritable value,
                                int numPartitions) {
            // TODO: implement getPartition such that pairs with the same first element
            //       will go to the same reducer. You can use toUnsighed as utility.

            return toUnsigned(key.getFirst().toString().hashCode()) % numPartitions;
            //return 1;
        }

        /**
         * toUnsigned(10) = 10
         * toUnsigned(-1) = 2147483647
         *
         * @param val Value to convert
         * @return the unsigned number with the same bits of val
         * */
        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }
    }


    static class RS2JoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {

        @Override
        public void map(LongWritable key, // TODO: change Object to input key type
                        Text value, // TODO: change Object to input value type
                        Context context)
                throws java.io.IOException, InterruptedException {

            // TODO: implement map method
            String line = value.toString();
            String[] words = line.split("\\s+");

            context.write(new TextPair(words[0], "1"), new Text(words[1]));
        }
    }


    class RSJCombiner extends Reducer<TextPair, Text, Text, ListRSJ> {
        @Override
        public void reduce(TextPair key, // TODO: change Object to input key type
                           Iterable<Text> values, // TODO: change Object to input value type
                           Context context) throws IOException, InterruptedException {

            Iterator<Text> iter = values.iterator();
            //Text uid = new Text(iter.next());
            ListRSJ reducers = new ListRSJ();
            ListRSJ followers = new ListRSJ();

            while(iter.hasNext()){
                Text record = iter.next();
                if(key.getSecond().equals(new Text("0"))){
                    reducers.set(record);
                }
                else{
                    if(!followers.contiene(record))
                        followers.set(record);
                }
            }

            for(int i=0 ; i<reducers.dimensione(); i++){
                context.write(reducers.get(i), followers);
            }
        }
    }




    // TODO: implement reducer
    static class RSJoinReducer extends Reducer<Text, ListRSJ, Text, ListRSJ> {

        @Override
        public void reduce(Text key, // TODO: change Object to input key type
                           Iterable<ListRSJ> values, // TODO: change Object to input value type
                           Context context) throws IOException, InterruptedException {

            Iterator<ListRSJ> iter = values.iterator();
            ListRSJ l = new ListRSJ();

            while(iter.hasNext()){
                ListRSJ record = iter.next();
                for(int i=0; i<record.dimensione(); i++) {
                    if (!l.contiene(record.get(i))) {
                        l.set(record.get(i));
                    }
                }
            }
            context.write(key, l);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        // TODO: implement all the job components andconfigurations

        Configuration conf = getConf();

        // TODO: add the smallFile to the distributed cache



        Job job = new Job(conf,"RSJoin"); // TODO: define new job instead of null using conf e setting
        // a name

        // TODO: set job input format
        MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, RS1JoinMapper.class);
        MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, RS2JoinMapper.class);

        // TODO: set the output path for the job results (to HDFS) to the variable
        FileOutputFormat.setOutputPath(job, new Path(args[3]));


        // TODO: set map class and the map output key and value classes
        //job.setMapperClass(DCJoinMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        //job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(PartitionerRSJ.class); //TODO u ficiumu niautri


        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(RSJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ListRSJ.class);

        // * TODO: set the combiner class and the combiner output key and value classes
        job.setCombinerClass(RSJCombiner.class);


        // outputPath
        job.setNumReduceTasks(Integer.parseInt(args[0]));// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(WordCount.class); // TODO: set the jar class
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public ReduceSideJoin(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
        System.exit(res);
    }
}