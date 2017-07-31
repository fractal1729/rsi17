package keyvaltest;

// This is the class which sends requests to replicas
import bftsmart.tom.ServiceProxy;

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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MapClient implements Map<String, String> {

    ServiceProxy clientProxy = null;
    PrintWriter logger = null;

    public MapClient(int clientId) throws IOException {
        clientProxy = new ServiceProxy(clientId);
        logger = new PrintWriter(new BufferedWriter(new FileWriter("clientlog"+clientId+".out")));
    }

    private void log(String message) {
        logger.println(System.currentTimeMillis()+"\t"+message);
    }

    private String condenseBlock(String value) { // may be replaced by hash function or the first n characters.
        return value;
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String put(String key, String value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        try {
            log("Gathering PUT request");
            dos.writeInt(RequestType.PUT);
            dos.writeUTF(key);
            dos.writeUTF(value);
            log("Submitting PUT request");
            byte[] reply = clientProxy.invokeOrdered(out.toByteArray());
            if (reply != null) {
                String previousValue = new String(reply);
                return previousValue;
            }
            log("Reply received, PUT request successful");
            return null;
        } catch (IOException ioe) {
            System.out.println("Exception putting value into hashmap: " + ioe.getMessage());
            log("Exception putting value into hashmap: "+ioe.getMessage());
            return null;
        }
    }

    @Override
    public String get(Object key) {
        try {
            log("Gathering GET request");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(RequestType.GET);
            dos.writeUTF(String.valueOf(key));
            log("Submitting GET request");
            byte[] reply = clientProxy.invokeUnordered(out.toByteArray());
            String value = new String(reply);
            log("Reply received: "+condenseBlock(value));
            log("GET request successful");
            return value;
        } catch (IOException ioe) {
            System.out.println("Exception getting value from the hashmap: " + ioe.getMessage());
            log("Exception getting value from the hashmap: "+ioe.getMessage());
            return null;
        }
    }

    @Override
    public String remove(Object key) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        try {
            log("Gathering REMOVE request");
            dos.writeInt(RequestType.REMOVE);
            dos.writeUTF(String.valueOf(key));
            log("Submitting REMOVE request");
            byte[] reply = clientProxy.invokeOrdered(out.toByteArray());
            if (reply != null) {
                String removedValue = new String(reply);
                log("Removed value: "+condenseBlock(removedValue));
                log("REMOVE request succesful");
                return removedValue;
            }
            log("No value was removed");
            log("REMOVE request succesful");
            log("Closing log file");
            logger.close();
            System.exit(0);
            return null;
        } catch (IOException ioe) {
            System.out.println("Exception removing value from the hashmap: " + ioe.getMessage());
            ioe.printStackTrace(System.out);
            log("Exception removing value from the hashmap: "+ioe.getMessage());
            return null;
        }
    }

    @Override
    public int size() {
        try {
            log("Gathering SIZE request");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(RequestType.SIZE);
            log("Submitting SIZE request");
            byte[] reply = clientProxy.invokeUnordered(out.toByteArray());
            ByteArrayInputStream in = new ByteArrayInputStream(reply);
            DataInputStream dis = new DataInputStream(in);
            int size = dis.readInt();
            log("Size received: "+size);
            log("SIZE request succesful");
            return size;
        } catch (IOException ioe) {
            System.out.println("Exception getting the size of the hashmap: " + ioe.getMessage());
            log("Exception getting the size of the hashmap: "+ioe.getMessage());
            return -1;
        }
    }
}
