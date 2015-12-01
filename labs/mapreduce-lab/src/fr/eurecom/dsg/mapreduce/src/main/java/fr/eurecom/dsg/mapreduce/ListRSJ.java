package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
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
public class ListRSJ implements Writable {

    // TODO: add an internal field that is the real associative array
    private ArrayList<Text> value;

    public void set(Text t) {
        this.value.add(t);
    }

    public Text get(int index) {
        return this.value.get(index);
    }

       public ListRSJ() {
        this.value = new ArrayList<Text>();
    }

    @Override
    public String toString() {
        // TODO: implement toString for text output format
        String conc="";

        for(int i=0; i < value.size(); i++){
            conc+=" "+ value.get(i);
        }
        return conc;
    }

    public boolean contiene(Text t){
        return value.contains(t);
    }

    public int dimensione(){
        return value.size();
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
            this.value.add(key);
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
        for(int j=0; j < value.size(); j++){
            t = value.get(j);
            t.write(out);
        }
    }
}
