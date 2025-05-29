import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    static HashMap<String, String> map= new HashMap<>();

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        int port = 6379;
        try(ServerSocket serverSocket = new ServerSocket(port)){
            serverSocket.setReuseAddress(true);
            while(true) {
                Socket client = serverSocket.accept();
                ExecutorService threadPool = Executors.newCachedThreadPool();
                threadPool.submit(()-> {
                    try {
                        HandleRedisClient handleRedisClient=new HandleRedisClient(client);
                        handleRedisClient.handleClient();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }catch(IOException e){
          System.out.println("Outer IOException: " + e.getMessage());
        }
  }

}
