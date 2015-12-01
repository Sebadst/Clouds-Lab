package fr.eurecom.dsg.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


/**
 * Created by campo on 10/28/2015.
 */
public class OrderInversion extends Configured implements Tool {


    private final static String ASTERISK = "\0";


    public static class PartitionerTextPair extends
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


    public static class PairMapperOI extends
            Mapper<LongWritable, Text, TextPair, IntWritable> {

        private IntWritable ONE = new IntWritable(1);
        private TextPair textValue = new TextPair();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            // TODO: implement the map method
            String line = value.toString();
            String[] words = line.split("\\s+");

            for(String word : words) {
                int count = 0;
                for(String word2 : words){
                    if(!word.equals(word2)){
                        count++;
                        textValue.set(new Text(word), new Text(word2));
                        context.write(textValue,ONE);
                    }
                }
                textValue.set(new Text(word), new Text(ASTERISK));
                context.write(textValue, new IntWritable(count));
            }
        }
    }


    public static class PairReducerOI
            extends Reducer<TextPair,
            IntWritable,
            TextPair,
            DoubleWritable> {

        // TODO: implement the reduce method
        private DoubleWritable writableSum = new DoubleWritable();
        private int div = 0;
        private String prec = "sbgkjerbgklserjvgl";
        double wordCount;

        @Override
        protected void reduce(TextPair key, // TODO: change Object to input key type
                              Iterable<IntWritable> values, // TODO: change Object to input value type
                              Context context) throws IOException, InterruptedException {

            /*// TODO: implement the reduce method (use context.write to emit results)
            int sum = 0;
            for (IntWritable value : values){
                if(!key.getFirst().toString().equals(prec)){
                    this.div = 0;
                    prec = key.getFirst().toString();
                }
                if(key.getSecond().toString().equals(ASTERISK)){
                    this.div += value.get();
                    continue;
                }
                sum += value.get();
            }

            if(key.getSecond().toString().equals(ASTERISK)){
                return;
            }
            //else
            writableSum.set(sum/div);
            context.write(key, writableSum);*/

            Iterator<IntWritable> it = values.iterator();
            double count = 0;
            while (it.hasNext()) {
                count += it.next().get();
            }

            if(key.getSecond().toString().compareTo(ASTERISK) == 0){
                wordCount = count;
            }
            else {
                TextPair p = new TextPair();
                p.set(new Text(key.getFirst()), new Text(key.getSecond()));
                context.write(p, new DoubleWritable(count/wordCount));
            }
        }
    }


    private int numReducers;
    private Path inputPath;
    private Path outputDir;


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();


        Job job = new Job(conf,"order inversion");  // TODO: define new job instead of null using conf e setting a name

        job.setInputFormatClass(TextInputFormat.class);// TODO: set job input format

        job.setMapperClass(PairMapperOI.class);// TODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(PartitionerTextPair.class); //TODO u ficiumu niautri

        job.setReducerClass(PairReducerOI.class);// TODO: set reduce class and the reduce output key and value classes
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);// TODO: set job output format

        FileInputFormat.addInputPath(job, this.inputPath);// TODO: add the input file as job input (from HDFS) to the variable
        FileOutputFormat.setOutputPath(job, this.outputDir);// TODO: set the output path for the job results (to HDFS) to the variable
        job.setNumReduceTasks(this.numReducers);// TODO: set the number of reducers using variable numberReducers
        job.setJarByClass(OrderInversion.class);// TODO: set the jar class

        return job.waitForCompletion(true) ? 0 : 1;
    }


    OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}
