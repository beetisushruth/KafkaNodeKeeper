package main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import consumer.Consumer;
import producer.Producer;

public class KafkaNodeKeeper {
	private static Logger logger = LoggerFactory.getLogger(KafkaNodeKeeper.class);
	private static int threshold = 1500;
	private static int globalMaximum = 0;
	private static Map<String, Integer> map = new HashMap<>();
	
	public static void main(String[] args) {
		try {
			String producerConfigFile = args[0];
			String consumerConfigFile = args[1];
			int n = args.length, i = 0;
			while(i < n) {
				String solver = args[i];
				map.put(solver, 0);
				i++;
			}
			Producer.setupProducer(producerConfigFile);
			Consumer.setupConsumer(consumerConfigFile);
			while (globalMaximum <= threshold) {
				List<String> input = Consumer.consume(consumerConfigFile);
				if (input != null && input.size() > 0) {
					// best solution keeping logic
					String latestValueFromTopic = input.get(input.size() - 1);
					JSONObject jsonObject = new JSONObject(latestValueFromTopic);
					int bestTopicSol = Integer.parseInt((String) jsonObject.get("sol"));
					String solver = (String) jsonObject.get("producedBy");
					if(bestTopicSol > globalMaximum) {
						globalMaximum = bestTopicSol;
						map.put(solver, bestTopicSol);
						produceEvents(solver);
					}
					Producer.produce(producerConfigFile, "");
				}
			}
		} catch (Exception e) {
			logger.error("Error occured while node keeping " + e.getMessage());
		}
	}

	private static void produceEvents(String solver) {
		
		
	}
}
