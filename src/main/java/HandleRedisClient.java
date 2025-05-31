import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.*;

public class HandleRedisClient {
    private static final ConcurrentHashMap<String, String> map=new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> deleteMap=new ConcurrentHashMap<String, Integer>();
    private static final ExecutorService executor= Executors.newCachedThreadPool();
    private final BufferedReader reader;
    private final BufferedWriter writer;
    private int numArgs;

    HandleRedisClient(Socket client) throws IOException {
        System.out.println("Client connected: " + client.getInetAddress().getHostAddress());
        this.reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
        this.numArgs=0;
    }

    public void handleClient() throws IOException {
        String line;
        while((line = reader.readLine())!=null){
            System.out.println("Received request: " + line);
            if(line.startsWith("*")){
                this.numArgs = Integer.parseInt(line.substring(1));
                reader.readLine();
                line= reader.readLine();
                String command=line;
                if(this.numArgs==1){
                    if ("ping".equalsIgnoreCase(command)) {
                        this.sendResponseToClient("PONG");
                    }
                }
                else if(((this.numArgs==3 || this.numArgs==5) && "set".equalsIgnoreCase(command)) || (this.numArgs==2 && "get".equalsIgnoreCase(command))){
                    this.setOrGetKeyValPair(((this.numArgs==3 || this.numArgs==5) && "set".equalsIgnoreCase(command)));
                }
                else{
                    System.out.println(command);
                    if("echo".equalsIgnoreCase(command)){
                        this.echoResponse();
                    }
                }
            }
        }
    }

    private void addKeyWithExpiry(String key,int milliseconds) throws InterruptedException {
        System.out.println("Timeout: "+milliseconds);
        executor.submit(()->{
            try {
                Thread.sleep(milliseconds);
                System.out.println("Sleep executed successfully");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            map.remove(key);
        });
        System.out.println("Async task submitted...");

//        executor.shutdown();
//        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void setOrGetKeyValPair(boolean setValue) throws IOException {
        System.out.println("setValue+ "+setValue);
        String line= this.reader.readLine();
        System.out.println(line);
        String key= this.reader.readLine();
        if(setValue){
            line= this.reader.readLine();
            System.out.println(line);
            String val= reader.readLine();
            if(this.numArgs==5){
                System.out.println("Set command with Expiry");
                line= this.reader.readLine();
                System.out.println(line);
                String expArg= reader.readLine();
                System.out.println("expArgs: "+expArg);
                if("px".equalsIgnoreCase(expArg)){
                    try{
                        line= this.reader.readLine();
                        System.out.println(line);
                        String expTimeoutArg= reader.readLine();
                        int expTimeout= Integer.parseInt(expTimeoutArg);
                        CompletableFuture.runAsync(()->{
                            try {
                                this.addKeyWithExpiry(key,expTimeout);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        System.out.println("Main continues immediately...");
                    }catch (Exception e){
                        this.sendResponseToClient("Invalid args: "+e);
                    }
                }else{
                    this.sendResponseToClient("Invalid Argument(s)");
                }
            }
            map.put(key,val);
            System.out.println("value: "+map.get(key));
            this.sendResponseToClient("OK");
        }else{
            System.out.println("key: "+key);
            String response=map.getOrDefault(key, null);
            System.out.println("response: "+response);
            this.sendResponseToClient(response);
        }

    }

    private void echoResponse() throws IOException {
        ArrayList<String> resultList=new ArrayList<>();
        String line;
        for(int i=0;i<this.numArgs-1;i++){
            line= reader.readLine();
            System.out.println(line);
            resultList.add(reader.readLine());
        }
        this.sendResponseToClient(String.join(" ",resultList));
    }

    private void sendResponseToClient(String text) throws IOException {
        String result=text!=null?"+"+text+"\r\n":"$-1\r\n";
        this.writer.write(result);
        this.writer.flush();
        System.out.println("Sent response to client: "+result);
    }
}
