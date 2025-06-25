import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

public class ParseRdbFile {

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

    public void loadRdbFile(String dir, String dbFileName, ConcurrentHashMap<String, String> map) {
        System.out.println("loading RDB");
        File rdbFile= Paths.get(dir + "/" + dbFileName).toFile();
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



            int i = 0;

            while(i<numberOfExpiries+numberOfKeys){
                byte flag = fis.readNBytes(1)[0];
                i++;
                switch (flag){
                    case EXPIRE_MILLIS:{ fis.skip(8);}
                    case EXPIRE_SECONDS:{ fis.skip(4);}
                    case STRING:{
                        int keyLength = fis.read();
                        String key = new String(fis.readNBytes(keyLength), 0, keyLength, StandardCharsets.US_ASCII);

                        int valueLength = fis.read();
                        String value = new String(fis.readNBytes(valueLength), 0, valueLength,StandardCharsets.US_ASCII);
                        if(key.isEmpty()){
                            continue;
                        }
                        map.put(key, value);
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
}
