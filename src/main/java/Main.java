import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        int port = 6379;
        try(ServerSocket serverSocket = new ServerSocket(port)){
            serverSocket.setReuseAddress(true);
            while(true) {
                try (Socket client = serverSocket.accept()) {
                    System.out.println("Client connected: " + client.getInetAddress().getHostAddress());

                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
                    writer.write("+PONG\r\n");
                    writer.flush();
//                    System.out.println("Sent response to client");
                }
            }
        }catch(IOException e){
          System.out.println("Outer IOException: " + e.getMessage());
        }
  }
}
