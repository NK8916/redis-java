import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

public class HandleRedisClient {
    private static final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiryTimes = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ExecutorService executor= Executors.newCachedThreadPool();
    private String dir;
    private String dbFileName;
    private final BufferedReader reader;
    private final BufferedWriter writer;
    private int numArgs;

    // Section Flags
    private static final byte METADATA = (byte)0xfa;
    private static final byte DATABASE = (byte)0xfe;
    private static final byte HASH_TABLE_SIZE = (byte)0xfb;
    private static final byte EXPIRE_MILLIS = (byte)0xfc;
    private static final byte EXPIRE_SECONDS = (byte)0xfd;

    // Data Type Flags
    private static final byte STRING = 0x00;
    private static final byte LIST = 0x01;
    private static final byte SET = 0x02;
    private static final byte SORTED_SET = 0x03;
    private static final byte HASH = 0x04;
    private static final byte ZIPMAP = 0x09;
    private static final byte ZIPLIST = 0x0a;
    private static final byte INTSET = 0x0b;
    private static final byte SORTED_SET_ZIPLIST = 0x0c;
    private static final byte HASHMAP_ZIPLIST = 0x0d;
    private static final byte LIST_QUICKLIST = 0x0e;

    HandleRedisClient(String[] args,Socket client) throws IOException {
        System.out.println("Client connected: " + client.getInetAddress().getHostAddress());
        this.reader = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
        this.numArgs=0;
        for(int i=0;i<args.length;i++){
            if("--dir".equalsIgnoreCase(args[i]) && i<args.length-1){
                this.dir=args[++i];
            }else if("--dbfilename".equalsIgnoreCase(args[i]) && i<args.length-1){
                this.dbFileName=args[++i];
            }else{
                throw new IllegalArgumentException("Invalid Arguments");
            }
        }

        loadRdbFile();
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
                else if(this.numArgs==2 && "keys".equalsIgnoreCase(command)){
                    this.processKeys();
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

    private void processKeys() throws IOException {
        String line= this.reader.readLine();
        System.out.println(line);
        line= this.reader.readLine();
        System.out.println("after line: "+line);
        if(line.contains("*")){
            ArrayList<String> stringList=new ArrayList<>();
            if(line.equalsIgnoreCase("*")){
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String key=entry.getKey();
                    stringList.add(key);
                }
            }else{
                int idx=line.indexOf("*");
                String partialString=line.substring(0,idx);
                System.out.println("idx: "+idx);
                System.out.println("partialString: "+partialString);
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String key=entry.getKey();
                    System.out.println("key: "+key);
                    if(idx>=key.length()){
                        continue;
                    }
                    String substring=key.substring(0,idx);
                    if(substring.equalsIgnoreCase(partialString)){
                        stringList.add(key);
                    }
                }
            }

            this.sendResponseToClient(createRESPArray(stringList.toArray(new String[0])));
        }
    }

    private void loadRdbFile(){
        System.out.println("loading RDB");
        File rdbFile= Paths.get(this.dir + "/" + this.dbFileName).toFile();
        if(!rdbFile.exists()){
            System.out.println("RDB file doesn't exist, initializing empty store");
            return;
        }

        try(FileInputStream fis=new FileInputStream(rdbFile)){
            byte[] header = fis.readNBytes(9);
            System.out.println(new String(header));

            byte subsectionHeader;

            while ((subsectionHeader = fis.readNBytes(1)[0]) == METADATA){
                System.out.println("Check if following is metadata header 0xfa");
                String hex = String.format("%02x", subsectionHeader);
                System.out.println(hex);
                int metadataKeyLength = decodeLength(fis);
                System.out.println("metadata key length: " + metadataKeyLength);
                fis.skip(metadataKeyLength);
                int metadataValueLength = decodeLength(fis);
                System.out.println("metadata value length: " + metadataValueLength);
                fis.skip(metadataValueLength);
            }

            assert subsectionHeader==DATABASE;

            System.out.println("Check if following is database header 0xfe");
            String hex = String.format("%02x", subsectionHeader);
            System.out.println(hex);

            fis.skip(1);

            subsectionHeader = fis.readNBytes(1)[0];
            assert subsectionHeader == HASH_TABLE_SIZE;

            System.out.println("Check if following is hash table size header 0xfb");
            hex = String.format("%02x", subsectionHeader);
            System.out.println(hex);

            int numberOfKeys = fis.readNBytes(1)[0];
            int numberOfExpiries = fis.readNBytes(1)[0];

            System.out.println("number of keys: " + numberOfKeys);
            System.out.println("number of expiries: " + numberOfExpiries);

            byte flag = fis.readNBytes(1)[0];

            int i = 0;

            while(i<numberOfExpiries+numberOfKeys){
                i++;

                switch (flag){
                    case EXPIRE_MILLIS:{ fis.skip(8);}
                    case EXPIRE_SECONDS:{ fis.skip(4);}
                    case STRING:{
                        int keyLength = fis.read();
                        String key = new String(fis.readNBytes(keyLength), 0, keyLength);

                        int valueLength = fis.read();
                        String value = new String(fis.readNBytes(valueLength), 0, valueLength);
                        map.put(key,value);
                        System.out.println("Key: "+key+" Value: "+value);
                    }
                    default:{}
                }
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException();
        }
    }

    private int decodeLength(FileInputStream fis) throws IOException{
        int firstByte = fis.read();
        int firstBitMask = 1 << 7;
        int secondBitMask = 1 << 6;
        boolean firstBitIsSet = (firstByte & firstBitMask) != 0;
        boolean secondBitIsSet = (firstByte & secondBitMask) != 0;

        if(firstBitIsSet){
            if(secondBitIsSet){
                int formatMask = 0b00111111;
                int format = firstByte & formatMask;

                if (format > 2) {
                    throw new UnsupportedOperationException();
                }

                System.out.println("format: " + format);
                System.out.println("read this many bytes: " + (int) Math.pow(2, format));
                return (int) Math.pow(2, format);
            }else{
                byte[] length = fis.readNBytes(4);
                return ByteBuffer.wrap(length).order(ByteOrder.LITTLE_ENDIAN).getInt();
            }
        }else{
            if (secondBitIsSet) {
                // 01: Read one additional byte. The combined 14 bits represent the length.
                int modifiedFirstByteMask = 0b00111111;
                byte modifiedFirstByte = (byte) (modifiedFirstByteMask & firstByte);
                byte secondByte = (byte) fis.read();
                return ByteBuffer.wrap(new byte[] {modifiedFirstByte, secondByte}).order(ByteOrder.LITTLE_ENDIAN).getInt();

            } else {
                // 00 : The next 6 bits represent the length.
                return firstByte;
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
