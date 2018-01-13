package storm.starter;

import java.io.*;
import java.net.Socket;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class ServerAndThreadCoordinationUtils {

    //set max time to run.
    public static void setMaxTimeToRunTimer(int millisecs) {
        Date timeLimit =
                new Date(new Date().getTime() + millisecs);
        Timer timer = new Timer();

        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                System.out.println("aborting test !  Took too long");
                System.exit(-1);
            }
        }, timeLimit);
    }


    public static String send4LetterWord(String host, int port, String cmd)
            throws IOException {
        System.out.println("connecting to " + host + " " + port);
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outputStream = sock.getOutputStream();
            outputStream.write(cmd.getBytes());
            outputStream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader =
                    new BufferedReader(
                            new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static boolean waitForServerUp(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                String result = send4LetterWord(host, port, "stat");
                System.out.println("result of send: " + result);
                if (result.startsWith("Zookeeper version:")) {
                    return true;
                }
            } catch (IOException e) {
                // ignore as this is expected
                System.out.println("server " + host + ":" + port + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

}
