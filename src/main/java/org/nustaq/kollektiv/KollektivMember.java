package org.nustaq.kollektiv;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.remoting.base.*;
import org.nustaq.kontraktor.remoting.tcp.*;
import org.nustaq.kontraktor.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.locks.*;

import com.beust.jcommander.*;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMember extends Actor<KollektivMember> {

    public static int HB_MILLIS = 1000;
    public static final int DEFAULT_PORT = 3456;

    KollektivMaster master;
    String nodeId;
    ActorAppBundle app;
    MasterDescription masterDescription;

    Options options;
    Log localLog;

    public void $init(Options options) {
        this.options = options;
        nodeId = options.getName()+"_"+MemberDescription.findHost()+"_"+Integer.toHexString((int)(System.currentTimeMillis()&0xffff));
        localLog = Actors.AsActor(Log.class,10000);
        localLog.setSeverity(Log.WARN);
        $connectLoop();
    }

    boolean tryConnect = false;
    public void $connectLoop() {
        String s = options.getMasterAddr();
        if ( master == null ) {
            if (!tryConnect) {
                tryConnect = true;
                Log.Warn(this, "trying to connect " + s);
            }
            String[] split = s.split(":");
            try {
                Callback<ActorClientConnector> disconnected = (r, e) -> {
                    Log.Info(null, "master disconnected ");
                    master = null;
                    tryConnect = false;
                };
                master = (KollektivMaster) new TCPConnectable(
                        KollektivMaster.class,
                        split[0],
                        Integer.parseInt(split[1])
                    )
                    .connect(disconnected)
                    .await();
                Log.Info(null, "master connected ..");
                tryConnect = false;
                master.$registerMember(new MemberDescription(self(), nodeId, options.getAvailableProcessors()))
                    .onResult(md -> {
                        masterDescription = md;
                        tryConnect = false;
                        if (options.remoteLog) {
                            Log.Lg.setLogWrapper(
                                (Thread t, int severity, Object source, Throwable ex, String msg) -> {
                                    String exString = null;
                                    if (ex != null) {
                                        StringWriter sw = new StringWriter();
                                        PrintWriter pw = new PrintWriter(sw);
                                        ex.printStackTrace(pw);
                                        exString = sw.toString();
                                    }
                                    if (master.isStopped()) {
                                        if (Log.Lg.getSeverity() <= severity)
                                            Log.Lg.defaultLogger.msg(t, severity, source, ex, msg);
                                    } else {
                                        master.$remoteLog(severity, nodeId + "[" + source + "]", exString == null ? msg : msg + "\n" + exString);
                                    }
                                }
                            );
                            Log.Lg.warn(this, " start logging from " + nodeId);
                        }
                    })
                    .onError(err -> {
                        tryConnect = false;
                        master = null;
                        localLog.Warn(this, "registering failed " + err);
                    });
            } catch (Exception e) {
                tryConnect = false;
                Log.Info(this, "connection failed");
            }
        }
        delayed(HB_MILLIS, () -> $connectLoop() );
    }

    public void $refDisconnected(String address, ActorClientConnector disconnectedRef) {
        if ( disconnectedRef == master ) {
            Log.Lg.resetToSysout();
            master = null;
        }
        localLog.warn(this, "actor disconnected " + disconnectedRef + " address:" + address + " master: " + master);
    }

    private boolean tryDelRecursive(File base) {
        if ( ! base.exists() )
            return true;
        try {
            int count[] = {0};
            int prevCount;
            do {
                prevCount = count[0];
                count[0] = 0;
                if ( ! base.exists() )
                    return true;
                Files.walk(Paths.get(base.getAbsolutePath()), 65536).forEach(path -> {
                    count[0]++;
                    File file = path.toAbsolutePath().toFile();
                    file.delete();
                });
            } while( count[0] != prevCount );
        } catch (Exception e) {
            localLog.warnLong(this, e, null);
        }
        base.delete();
        return !base.exists();
    }

    public IPromise $install(ActorAppBundle bundle) {
        try {
            File base = new File( options.getTmpDirectory() + File.separator + nodeId+File.separator+bundle);
            int count = 0;
            while ( ! tryDelRecursive(base) ) {
                base = new File( base.getParent() + File.separator + bundle+"_"+ count++);
            }
            base.mkdirs();
            final File finalBase = base;
            bundle.getResources().entrySet().forEach(entry -> {
                if (entry.getKey().endsWith(".jar")) {
                    String name = new File(entry.getKey()).getName();
                    try {
                        Files.write(Paths.get(finalBase.getAbsolutePath(), name), entry.getValue().bytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        new File(finalBase.getAbsolutePath() + File.separator + entry.getKey()).getParentFile().mkdirs();
                        Files.write(Paths.get(finalBase.getAbsolutePath(), entry.getKey()), entry.getValue().bytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            bundle.setBaseDir(base.getAbsolutePath());
            app = bundle;
            localLog.warn(this, "defined app bundle space " + bundle + " size " + bundle.getSizeKB() + " filebase:" + base.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Promise<>(null);
    }

    public void $restart(long timeMillis) {
        Log.Warn(this, "restarting in " + timeMillis + " ms");
        ArrayList<String> cmd = new ArrayList();
        cmd.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        for (String jvmArg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            cmd.add(jvmArg);
        }
        cmd.add("-cp");
        cmd.add(ManagementFactory.getRuntimeMXBean().getClassPath());
        cmd.add(KollektivMember.class.getName());
        for (String arg : args) {
            cmd.add(arg);
        }
        delayed(timeMillis, () -> {
            try {
                final ProcessBuilder builder = new ProcessBuilder(cmd);
                builder.inheritIO();
                builder.start();
//                Runtime.getRuntime().exec(cmd.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        });
    }

    public void $terminate(long timeMillis) {
        localLog.warn(this, "terminating in "+timeMillis+" ms");
        delayed(timeMillis, () -> {
            System.exit(0);
        });
    }

    public static class Options {
        @Parameter( names = {"-c","-cores"}, description = "specify how many cores should be available. (Defaults to number of cores reported by OS).")
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        @Parameter( names = {"-n", "-name"}, description = "optionally specify a symbolic name which can be used as a hint from the master process.")
        String name = "Node";
        @Parameter( names = { "-t", "-tmp"}, description = "specify temporary directory to deploy downloaded classes/jars. Defaults to /tmp")
        String tmpDirectory = "/tmp";
        @Parameter( names = {"-m", "-master"}, description = "define master addr:port. Defaults to 127.0.0.1:"+KollektivMember.DEFAULT_PORT)
        String masterAddr = "127.0.0.1:"+KollektivMember.DEFAULT_PORT;
        @Parameter( names = {"-rl", "remoteLog"}, description = "redirect logging to master node")
        boolean remoteLog = false;
        @Parameter(names = {"-h","-help","-?", "--help"}, help = true, description = "display help")
        boolean help;

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public String getName() {
            return name;
        }

        public String getTmpDirectory() {
            return tmpDirectory;
        }

        public String getMasterAddr() {
            return masterAddr;
        }

        @Override
        public String toString() {
            return "Options{" +
                    "availableProcessors=" + getAvailableProcessors() +
                    ", name='" + name + '\'' +
                    ", tmpDirectory='" + tmpDirectory + '\'' +
                    ", masterAddr='" + masterAddr + '\'' +
                    '}';
        }
    }

    static String[] args;
    public static void main( String a[] ) {
        args = a;
        Options options = new Options();
        JCommander com = new JCommander();
        com.addObject(options);
        try {
            com.parse(a);
        } catch (Exception ex) {
            System.out.println("command line error: '"+ex.getMessage()+"'");
            options.help = true;
        }
        if ( options.help ) {
            com.usage();
            System.exit(-1);
        }

        Log.setLevel(Log.WARN);
        System.out.println("===================================");
        System.out.println("==       kollektiv.MEMBER        ==");
        System.out.println("===================================");
        System.out.println("");
        System.out.println("starting kollektiv member with "+options );
        KollektivMember sl = Actors.AsActor(KollektivMember.class);
        sl.$init(options);
    }


}
