package org.nutz.ssdb4j.replication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class ReplicationSSDMStream implements SSDBStream {

    private static Logger logger = Logger.getLogger("ReplicationSSDMStream");

    private static int CHECK_PER_SEC = 60;

    protected SSDBStream master;

    protected SSDBStream slave;

    public AtomicBoolean isOnline = new AtomicBoolean(true);
    
    public Thread checkThread;

    public ReplicationSSDMStream(SSDBStream master, SSDBStream slave) {
        this.master = master;
        this.slave = slave;
        checkThread= new Thread(new CheckRunnable());
        checkThread.setDaemon(true);
        checkThread.start();
    }
    
    private class CheckRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (!isOnline.get()) {
                    try {
                        slave.req(Cmd.ping);
                        isOnline.set(true);
                        logger.info("slave is online.");
                    } catch (Exception e) {
                        isOnline.set(false);
                        logger.info("slave is offline.");
                    }
                }
                try {
                    Thread.sleep(CHECK_PER_SEC * 1000);
                } catch (InterruptedException e) {
                    logger.info(e.getMessage());
                }
            }
        }

    }

    public Response req(Cmd cmd, byte[]... vals) {
        if (cmd.isSlave() && isOnline.get()) {
            try {
                return slave.req(cmd, vals);
            } catch (Exception e) {
                logger.info("slave error: " + e);
                isOnline.set(false);
            }
        }
        return master.req(cmd, vals);
    }

    public void callback(SSDBStreamCallback callback) {
        master.callback(callback);
    }

    public void close() throws IOException {
        try {
            master.close();
        } finally {
            slave.close();
        }
    }
}
