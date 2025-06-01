import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.*;

public class HandleRedisClient {
    private static final ConcurrentHashMap<String, String> map=new ConcurrentHashMap<>();
    private static final ExecutorService executor= Executors.newCachedThreadPool();
    private String dir;
    private String dbFileName;
    private final BufferedReader reader;
    private final BufferedWriter writer;
    private final String[] args;
    private int numArgs;

    HandleRedisClient(String[] args,Socket client) throws IOException {
        System.out.println("Client connected: " + client.getInetAddress().getHostAddress());
        this.reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
        this.numArgs=0;
        this.args=args;
        System.out.println("args:=>>");
        for(int i=0;i<args.length;i++){
            if("--dir".equalsIgnoreCase(args[i]) && i<args.length-1){
                this.dir=args[++i];
            }else if("--dbfilename".equalsIgnoreCase(args[i]) && i<args.length-1){
                this.dbFileName=args[++i];
            }else{
                throw new IllegalArgumentException("Invalid Arguments");
            }
        }
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
                        this.sendTextRespTextToClient("PONG");
                    }
                }
                else if(((this.numArgs==3 || this.numArgs==5) && "set".equalsIgnoreCase(command)) || (this.numArgs==2 && "get".equalsIgnoreCase(command))){
                    this.setOrGetKeyValPair(((this.numArgs==3 || this.numArgs==5) && "set".equalsIgnoreCase(command)));
                }
                else if(this.numArgs==3 && "config".equalsIgnoreCase(command)){
                    this.processConfig();
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

    private void processConfig() throws IOException {
        String line= this.reader.readLine();
        System.out.println(line);
        line= this.reader.readLine();
        if("get".equalsIgnoreCase(line)){
            line=this.reader.readLine();
            System.out.println(line);
            line=this.reader.readLine();
            if("dir".equalsIgnoreCase(line)){
                if(this.dir==null){
                    this.sendTextRespTextToClient(null);
                }
                String[] stringArray=new String[] {"dir",this.dir};
                this.sendResponseToClient(createRESPArray(stringArray));
            }else if("dbfilename".equalsIgnoreCase(line)){
                if(this.dbFileName==null){
                    this.sendTextRespTextToClient(null);
                }
                String[] stringArray=new String[] {"dbfilename",this.dbFileName};
                this.sendResponseToClient(createRESPArray(stringArray));
            }else{
                this.sendTextRespTextToClient("Invalid Arg(s)");
            }

        }
    }

    private String createRESPArray(String[] elements){

        int n= elements.length;
        StringBuilder result= new StringBuilder("*" + n + "\r\n");
        for(String item:elements){
            result.append("$").append(item.length()).append("\r\n").append(item).append("\r\n");
        }
        return result.toString();
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
                        this.sendTextRespTextToClient("Invalid args: "+e);
                    }
                }else{
                    this.sendTextRespTextToClient("Invalid Argument(s)");
                }
            }
            map.put(key,val);
            System.out.println("value: "+map.get(key));
            this.sendTextRespTextToClient("OK");
        }else{
            System.out.println("key: "+key);
            String response=map.getOrDefault(key, null);
            System.out.println("response: "+response);
            this.sendTextRespTextToClient(response);
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
        this.sendTextRespTextToClient(String.join(" ",resultList));
    }

    private void sendTextRespTextToClient(String text) throws IOException {
        String result=text!=null?"+"+text+"\r\n":"$-1\r\n";
        this.sendResponseToClient(result);
    }

    private void sendResponseToClient(String result) throws IOException {
        this.writer.write(result);
        this.writer.flush();
        System.out.println("Sent response to client: "+result);
    }
}
