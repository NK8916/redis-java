import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        int port = 6379;
        try(ServerSocket serverSocket = new ServerSocket(port)){
            serverSocket.setReuseAddress(true);

            try(Socket client=serverSocket.accept()){
                BufferedReader in=new BufferedReader(new InputStreamReader(client.getInputStream()));
                BufferedWriter out=new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
                String input;
                while((input=in.readLine())!=null){
                    if(input.equalsIgnoreCase("PING")){
                        out.write("+PONG\r\n");
                    }
                }
                out.flush();
            }catch(IOException e){
                System.out.println("IOException: " + e.getMessage());
            }

        }catch(IOException e){
          System.out.println("IOException: " + e.getMessage());
        }
//        try {
//          serverSocket = new ServerSocket(port);
//          // Since the tester restarts your program quite often, setting SO_REUSEADDR
//          // ensures that we don't run into 'Address already in use' errors
//          serverSocket.setReuseAddress(true);
//          // Wait for connection from client.
//          clientSocket = serverSocket.accept();
//          InputStream byteData = clientSocket.getInputStream();
//          byte[] response = "+PONG\r\n".getBytes();
//          clientSocket.getOutputStream().write(response);
//          System.out.println(response);
//        } catch (IOException e) {
//          System.out.println("IOException: " + e.getMessage());
//        }
//        finally {
//          try {
//            if (clientSocket != null) {
//                System.out.println("test");
//              clientSocket.close();
//            }
//          } catch (IOException e) {
//            System.out.println("IOException: " + e.getMessage());
//          }
//        }
  }
}
