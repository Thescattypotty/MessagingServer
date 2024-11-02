import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final Broker broker;

    public ClientHandler(Socket socket, Broker broker) {
        this.clientSocket = socket;
        this.broker = broker;
    }

    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                String[] request = inputLine.split(" ", 4);
                String action = request[0];
                String topicName = request[1];
                String response = null;

                switch (action.toLowerCase()) {
                    case "create":
                        int partitions = Integer.parseInt(request[2]);
                        response = broker.createTopic(topicName, partitions);
                        break;
                    case "publish":
                        int partitionIndex = Integer.parseInt(request[2]);
                        String message = request[3];
                        response = broker.publishMessage(topicName, partitionIndex, message);
                        break;
                    case "consume":
                        partitionIndex = Integer.parseInt(request[2]);
                        long offset = Long.parseLong(request[3]);
                        List<Message> messages = broker.consumeMessages(topicName, partitionIndex, offset);
                        StringBuilder builder = new StringBuilder();
                        for (Message msg : messages) {
                            builder.append("Offset: ").append(msg.getOffset())
                                    .append(", Message: ").append(msg.getContent()).append("\n");
                        }
                        response = builder.toString();
                        break;
                    default:
                        response = "Unknown command.";
                }
                out.println(response);
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port or listening for a connection.");
            System.out.println(e.getMessage());
        }
    }
}
