package keyvaltest;

import java.io.Console;
import java.io.IOException;

public class AutoClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            System.out.println("Usage: AutoClient <client id> <number of blocks> <block size (bytes)> <block interval (ms)>");
            System.exit(0);
        }
        if(Integer.parseInt(args[1]) > 1000000) {
            System.out.println("Number of blocks must be between 1 and 1000000 inclusive.");
            System.exit(0);
        }

        MapClient client = new MapClient(Integer.parseInt(args[0]));
        int numBlocks = Integer.parseInt(args[1]);
        int blockSize = Integer.parseInt(args[2]);
        int blockInterval = Integer.parseInt(args[3]);
        
        for(int i = 0; i < numBlocks; i++) {
            String block = generateBlock(blockSize, i);
            client.put("block", block);
            Thread.sleep(blockInterval);
        }
        client.remove("block");
        System.exit(0);
    }

    private static String generateBlock(int blockSize, int blockid) {
        if(blockSize < 6) blockSize = 6;
        String block = String.format("%06d", blockid);
        char[] carr = new char[blockSize-6];
        block += new String(carr);
        return block;
    }
}