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

//  private static void handleClient(Socket client) throws IOException {
//      System.out.println("Client connected: " + client.getInetAddress().getHostAddress());
//      BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
//      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
//
//      String line;
//
//      while((line = reader.readLine())!=null){
//          System.out.println("Received request: " + line);
//          if(line.startsWith("*")){
//              int numArgs = Integer.parseInt(line.substring(1));
//              reader.readLine();
//              line= reader.readLine();
//              String command=line;
//              if(numArgs==1){
//                  if ("ping".equalsIgnoreCase(command)) {
//                      writer.write("+PONG\r\n");
//                      writer.flush();
//                      System.out.println("Sent response to client: +PONG");
//                  }
//              }
//              else if(numArgs==3 && "set".equalsIgnoreCase(command)){
//                    line= reader.readLine();
//                    System.out.println(line);
//                    String key= reader.readLine();
//                    line= reader.readLine();
//                    System.out.println(line);
//                    String val= reader.readLine();
//                    map.put(key,val);
//                    String response=map.get(key);
//                  String result="+"+response+"\r\n";
//                  writer.write(result);
//                  writer.flush();
//                  System.out.println("Sent response to client: "+result);
//
//
//              } else if (numArgs==2 && "get".equalsIgnoreCase(command)){
//                  line= reader.readLine();
//                  System.out.println(line);
//                  String key= reader.readLine();
//                  String response=map.get(key);
//                  String result="+"+response+"\r\n";
//                  writer.write(result);
//                  writer.flush();
//                  System.out.println("Sent response to client: "+result);
//              }
//              else{
//                  System.out.println(command);
//                  if("echo".equalsIgnoreCase(command)){
//                      ArrayList<String> resultList=new ArrayList<>();
//                    for(int i=0;i<numArgs-1;i++){
//                        line= reader.readLine();
//                        System.out.println(line);
//                        resultList.add(reader.readLine());
//                    }
//                    String result="+"+String.join(" ",resultList)+"\r\n";
//                    writer.write(result);
//                    writer.flush();
//                    System.out.println("Sent response to client: "+result);
//                  }
//              }
//          }
//      }
//  }
}
