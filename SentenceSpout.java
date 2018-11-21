import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.Iterator;

//import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"dont have a cow man",
			"i dont think i like fleas"
	};

	private int index = 0;

	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(new Fields("sentence"));
	}

	public void open(Map config, TopologyContext context, 
			SpoutOutputCollector collector) 
	{
		this.collector = collector;
	}

	//Storm's event logging functionality will only work if the messageId is serializable via Kryo or 
	//the Serializable interface. The emitted values must be immutable. Hence, I am making use of UUID here 
	//because Java UUID class represents an immutable universally unique identifier and represents 128-bit value.
	
	public void nextTuple() 
	{
		UUID msgID = UUID.randomUUID();
		this.collector.emit(new Values(sentences[index]),msgID);
		index++;

		if (index >= sentences.length) 
		{
			index = 0;
		}

	}

	//This is the implementation of the ack method from ISpout interface
	//When Storm acks or fails a message, it will pass back to the spout the same message id to 
	//identify which tuple it is referring to. This message id is used to take the message off the queue
	//once an ack is received.
	
	public void ack(Object msgId) {
		//super.ack(msgId);
		System.out.println("[SentenceSpout] ack on msgId" + msgId);
	}
	

	//This is the implementation of the fail method from ISpout interface
	//When Storm acks or fails a message, it will pass back to the spout the same message id to 
	//identify which tuple it is referring to. This message id is used to have it put back on the queue
	//once a fail is received. The spout tuple at the root of the tree will be replayed later on if 
	//the word tuple failed to be processed downstream
		
	public void fail(Object msgId){
		//super.fail(msgId);
		System.out.println("[SentenceSpout] fail on msgId" + msgId);
	}
}