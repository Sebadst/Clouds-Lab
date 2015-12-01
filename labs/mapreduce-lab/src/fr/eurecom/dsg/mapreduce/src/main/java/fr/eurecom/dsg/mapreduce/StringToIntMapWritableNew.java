package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.txn.Txn;

/*
 * Very simple (and scholastic) implementation of a Writable associative array f
or String to Int
 *
 **/
public class StringToIntMapWritableNew implements Writable {

    private MapWritable assArray = new MapWritable();

    @Override
    public void readFields(DataInput in) throws IOException {
        assArray = new MapWritable();
        assArray.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        assArray.write(out);
    }
/*
    @Override
    public String toString() {
        // TODO: implement toString for text output format
        String conc="";

        for(Writable k : assArray.keySet()){
            conc+= k +" "+ assArray.get(k).toString();
        }

        return conc;
    }
*/
    public Writable add(Object word)  {
        Text key = null;
        if(word instanceof String)
            key = new Text((String )word);
        else if (word instanceof Text)
            key = (Text) word;
        else if (word instanceof Writable)
            key = (Text) word;

        if(key == null)
            throw new RuntimeException("Wrong argument passed to add function");


        IntWritable value = (IntWritable) assArray.get(key);

        if(value == null){
            return assArray.put(key,new IntWritable(1));
        }

        value.set(value.get() + 1);
        return assArray.put(key, value);
    }

    public void add(StringToIntMapWritableNew value) {
        for(Map.Entry entry : value.assArray.entrySet()){
            this.add(entry.getKey());
        }
    }
}