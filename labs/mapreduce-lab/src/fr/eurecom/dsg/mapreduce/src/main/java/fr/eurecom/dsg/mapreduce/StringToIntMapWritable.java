package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    // TODO: add an internal field that is the real associative array
    private HashMap<String, Integer> value;

    public void set(String s, Integer i) {
        this.value.put(s, i);
    }

    public Integer get(String s) {
        return this.value.get(s);
    }

    public Set<String> getKeySet(){
        return value.keySet();
    }

    public StringToIntMapWritable() {
        this.value = new HashMap<String, Integer>();
    }

    @Override
    public String toString() {
        // TODO: implement toString for text output format
        String conc="";
        for(String k : value.keySet()){
            conc+= k +" "+ value.get(k);
        }

        return conc;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO: implement deserialization
        // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
        // StringToIntMapWritable when reading new records. Remember to initialize your variables
        // inside this function, in order to get rid of old data.
        this.value.clear();
        IntWritable cnt = new IntWritable();
        cnt.readFields(in);
        for(int i = 0; i < cnt.get(); i++){
            Text key = new Text();
            key.readFields(in);
            IntWritable v = new IntWritable();
            v.readFields(in);
            this.value.put(key.toString(), v.get());
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: write to out the serialized version of this such that
        // can be deserializated in future. This will be use to write to HDFS
        IntWritable l = new IntWritable(this.value.size());
        Text t;
        IntWritable i;
        l.write(out);
        for(String k : value.keySet()){
            t = new Text(k);
            t.write(out);
            i = new IntWritable(this.value.get(k));
            i.write(out);
        }
    }
}
