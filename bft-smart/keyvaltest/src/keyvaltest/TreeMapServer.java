package keyvaltest;

// These are the classes which receive requests from clients
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;

// Classes that need to be declared to implement this
// replicated Map
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.TreeMap;
import java.util.Map;

public class TreeMapServer extends DefaultRecoverable {

    Map<String, String> table;
    PrintWriter logger = null;
    String currentBlock = "";

    public TreeMapServer(int id) throws IOException {
        logger = new PrintWriter(new BufferedWriter(new FileWriter("serverlog"+id+".out")));
        table = new TreeMap<>();
        new ServiceReplica(id, this, this);
        log("TreeMapServer created with id "+id);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: HashMapServer <server id>");
            System.exit(0);
        }
        
        new TreeMapServer(Integer.parseInt(args[0]));
    }

    private void log(String message) {
        logger.println(System.currentTimeMillis()+"\t"+message);
    }

    private String condenseBlock(String value) { // may be replaced by hash function or the first n characters.
        return value.substring(0,6);
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] command, MessageContext[] mcs) {

        byte[][] replies = new byte[command.length][];
        for (int i = 0; i < command.length; i++) {
            replies[i] = executeSingle(command[i], mcs[i]);
        }

        return replies;
    }

    private byte[] executeSingle(byte[] command, MessageContext msgCtx) {
        ByteArrayInputStream in = new ByteArrayInputStream(command);
        DataInputStream dis = new DataInputStream(in);
        int reqType;
        try {
            reqType = dis.readInt();
            if (reqType == RequestType.PUT) {
                String key = dis.readUTF();
                String value = dis.readUTF();
                log("Received PUT request: "+key+condenseBlock(value));
                //String oldValue = table.put(key, value);
                String oldValue = currentBlock;
                currentBlock = value;
                byte[] resultBytes = null;
                if (oldValue != null) {
                    log("Old value replaced successfully.");
                    resultBytes = oldValue.getBytes();
                }
                log("PUT request fulfilled");
                return resultBytes;
            } else if (reqType == RequestType.REMOVE) {
                log("Received REMOVE/END request");
                String key = dis.readUTF();
                //log("Unpacked key: "+key);
                //String removedValue = table.remove(key);
                String removedValue = currentBlock;
                //log("Removed value: "+condenseBlock(removedValue));
                byte[] resultBytes = null;
                if (removedValue != null) {
                    resultBytes = removedValue.getBytes();
                }
                //log("REMOVE request fulfilled");
                log("Closing log file");
                logger.close();
                System.exit(1);
                return resultBytes;
            } else {
                System.out.println("Unknown request type: " + reqType);
                log("Unknown request type: "+reqType);
                return null;
            }
        } catch (IOException e) {
            System.out.println("Exception reading data in the replica: " + e.getMessage());
            e.printStackTrace();
            log("Exception reading data in the replica: "+e.getMessage());
            return null;
        }
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        ByteArrayInputStream in = new ByteArrayInputStream(command);
        DataInputStream dis = new DataInputStream(in);
        int reqType;
        try {
            reqType = dis.readInt();
            if (reqType == RequestType.GET) {
                log("Received GET request");
                String key = dis.readUTF();
                log("Unpacked key: "+key);
                String readValue = table.get(key);
                log("Read value: "+condenseBlock(readValue));
                byte[] resultBytes = null;
                if (readValue != null) {
                    resultBytes = readValue.getBytes();
                }
                log("GET request fulfilled");
                return resultBytes;
            } else if (reqType == RequestType.SIZE) {
                log("Received SIZE request");
                int size = table.size();
                log("Calculated size: "+size);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                dos.writeInt(size);
                byte[] sizeInBytes = out.toByteArray();

                log("SIZE request fulfilled");
                return sizeInBytes;
            } else {
                System.out.println("Unknown request type: " + reqType);
                log("Unknown request type: "+reqType);
                return null;
            }
        } catch (IOException e) {
            System.out.println("Exception reading data in the replica: " + e.getMessage());
            e.printStackTrace();
            log("Exception reading data in the replica: "+e.getMessage());
            return null;
        }
    }

    @Override
    public void installSnapshot(byte[] state) {
        ByteArrayInputStream bis = new ByteArrayInputStream(state);
        try {
            log("Installing snapshot");
            ObjectInput in = new ObjectInputStream(bis);
            currentBlock = (String) in.readObject();
            in.close();
            bis.close();
            log("Snapshot installation successfully complete");
        } catch (ClassNotFoundException e) {
            System.out.print("Coudn't find Map: " + e.getMessage());
            e.printStackTrace();
            log("Couldn't find Map: "+e.getMessage());
        } catch (IOException e) {
            System.out.print("Exception installing the application state: " + e.getMessage());
            e.printStackTrace();
            log("Exception installing the application state: "+e.getMessage());
        }
    }

    @Override
    public byte[] getSnapshot() {
        try {
            log("Getting snapshot");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(currentBlock);
            out.flush();
            out.close();
            bos.close();
            log("Snapshot successfully recorded");
            return bos.toByteArray();
        } catch (IOException e) {
            System.out.println("Exception when trying to take a + "
                    + "snapshot of the application state" + e.getMessage());
            e.printStackTrace();
            log("Exception when trying to take a snapshot of the application state"+e.getMessage());
            return new byte[0];
        }
    }
}
