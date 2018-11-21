import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, 
            OutputCollector collector) {
        this.collector = collector;
    }

    //Instead of counting the frequency of each word in WordCountBolt instances , 
    //I am modifying WordCountBolt to just emit the tuples like - (Word,1L) . 
    //This means every word is emitted as tuple with count -> 1
    
    public void execute(Tuple tuple) {
    	try {
        String word = tuple.getStringByField("word");
        Long count = 1L;
        
        //Each word tuple is anchored by specifying the input tuple as the first argument to emit.
		//Since the word tuple is anchored, the spout tuple at the root of the tree will be replayed later 
		//on if the word tuple failed to be processed downstream.
        
        this.collector.emit(tuple , new Values(word, count));
        
        //Bolts must call the ack method on the OutputCollector for every tuple they 
        //process so that Storm knows when tuples are completed 
        //(and can eventually determine that its safe to ack the original spout tuples)
        
        this.collector.ack(tuple);
    	}catch(Exception e) {
    		
    		//In case of any failure , the fail method is invoked. This will fail the spout tuple appropriately.
    		this.collector.fail(tuple);
    	}
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
