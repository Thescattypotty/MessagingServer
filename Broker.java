
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    private final Map<String, List<Partition>> topics = new ConcurrentHashMap<>();
    private static final String STORAGE_DIR = "partitions/";

    public Broker() {
        File dir = new File(STORAGE_DIR);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }
    
    public synchronized String createTopic(String topicName, int numPartitions) {
        if (!topics.containsKey(topicName)) {
            List<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                String partitionFile = STORAGE_DIR + topicName + "-partition-" + i + ".log";
                partitions.add(new Partition(partitionFile));
            }
            topics.put(topicName, partitions);
            return "Topic '" + topicName + "' with " + numPartitions + " partitions created.";
        } else {
            return "Topic '" + topicName + "' already exists.";
        }
    }

    public synchronized String publishMessage(String topicName, int partitionIndex, String message) {
        List<Partition> partitions = topics.get(topicName);
        if (partitions == null || partitionIndex >= partitions.size()) {
            return "Invalid topic or partition.";
        }
        Partition partition = partitions.get(partitionIndex);
        partition.appendMessage(message);
        return "Message published to partition " + partitionIndex + " of topic '" + topicName + "'.";
    }

    public synchronized List<Message> consumeMessages(String topicName, int partitionIndex, long offset) {
        List<Partition> partitions = topics.get(topicName);
        if (partitions == null || partitionIndex >= partitions.size()) {
            return Collections.emptyList();
        }
        Partition partition = partitions.get(partitionIndex);
        return partition.readMessages(offset);
    }

}