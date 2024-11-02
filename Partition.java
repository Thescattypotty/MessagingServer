import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Partition {
    private final String partitionFile;
    private long nextOffset;
    private final List<Message> messages;
    private final Lock lock = new ReentrantLock();

    public Partition(String filePath) {
        this.partitionFile = filePath;
        this.messages = new ArrayList<>();
        this.nextOffset = 0;
        loadMessages();
    }

    private void loadMessages() {
        try (BufferedReader reader = new BufferedReader(new FileReader(partitionFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                messages.add(new Message(nextOffset++, line));
            }
        } catch (IOException e) {
            System.out.println("No existing messages found for partition.");
        }
    }

    public void appendMessage(String messageContent) {
        lock.lock();
        try {
            Message message = new Message(nextOffset++, messageContent);
            messages.add(message);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(partitionFile, true))) {
                writer.write(messageContent);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            lock.unlock();
        }
    }

    public List<Message> readMessages(long fromOffset) {
        lock.lock();
        try {
            List<Message> result = new ArrayList<>();
            for (Message message : messages) {
                if (message.getOffset() >= fromOffset) {
                    result.add(message);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public long getNextOffset() {
        return nextOffset;
    }

}