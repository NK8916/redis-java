import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class HandleRedisClient {
    private static HashMap<String, String> map=new HashMap<>();
    private final BufferedReader reader;
    private final BufferedWriter writer;

    HandleRedisClient(Socket client) throws IOException {
        System.out.println("Client connected: " + client.getInetAddress().getHostAddress());
        this.reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
    }

    public void handleClient() throws IOException {
        String line;
        while((line = reader.readLine())!=null){
            System.out.println("Received request: " + line);
            if(line.startsWith("*")){
                int numArgs = Integer.parseInt(line.substring(1));
                reader.readLine();
                line= reader.readLine();
                String command=line;
                if(numArgs==1){
                    if ("ping".equalsIgnoreCase(command)) {
                        this.sendResponseToClient("PONG");
                    }
                }
                else if((numArgs==3 && "set".equalsIgnoreCase(command)) || (numArgs==2 && "get".equalsIgnoreCase(command))){
                    this.setOrGetKeyValPair((numArgs==3 && "set".equalsIgnoreCase(command)));
                }
                else{
                    System.out.println(command);
                    if("echo".equalsIgnoreCase(command)){
                        this.echoResponse(numArgs);
                    }
                }
            }
        }
    }

    private void setOrGetKeyValPair(boolean setValue) throws IOException {
        System.out.println("setValue+ "+setValue);
        String line= this.reader.readLine();
        System.out.println(line);
        String key= reader.readLine();
        if(setValue){
            line= this.reader.readLine();
            System.out.println(line);
            String val= reader.readLine();
            map.put(key,val);
            System.out.println("value: "+map.get(key));
            this.sendResponseToClient("OK");
        }else{
            System.out.println("key: "+key);
            String response=map.getOrDefault(key, "Key Value Pair does not exists");
            System.out.println("response: "+response);
            this.sendResponseToClient(response);
        }

    }

    private void echoResponse(int numArgs) throws IOException {
        ArrayList<String> resultList=new ArrayList<>();
        String line;
        for(int i=0;i<numArgs-1;i++){
            line= reader.readLine();
            System.out.println(line);
            resultList.add(reader.readLine());
        }
        this.sendResponseToClient(String.join(" ",resultList));
    }

    private void sendResponseToClient(String text) throws IOException {
        String result="+"+text+"\r\n";
        this.writer.write(result);
        this.writer.flush();
        System.out.println("Sent response to client: "+result);
    }
}
