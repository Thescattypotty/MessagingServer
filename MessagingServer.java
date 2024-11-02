import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class MessagingServer {
    public static void main(String[] args) throws IOException{
        int port = 9099;
        Broker broker = new Broker();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Broker started on port " + port);
            while(true){
                Socket clientSocket = serverSocket.accept();
                new ClientHandler(clientSocket, broker).start();
            }
        }
    }    
}
