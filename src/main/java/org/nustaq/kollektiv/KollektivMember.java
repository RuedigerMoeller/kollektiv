package org.nustaq.kollektiv;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;
import org.nustaq.kontraktor.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

import com.beust.jcommander.*;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMember extends Actor<KollektivMember> {

    public static final int RETRY_INTERVAL_MILLIS = 1000;
    KollektivMaster master;
    String nodeId;
    ActorAppBundle app;
    MasterDescription masterDescription;

    Options options;
    Log localLog;

    public void $init(Options options) {
        this.options = options;
        nodeId = options.getName()+"_"+MemberDescription.findHost()+(System.nanoTime()&0xffff)+(int)(Math.random()*99);
        localLog = Actors.AsActor(Log.class,100000);
        localLog.$setSeverity(Log.WARN);
        $startHB();
    }

    boolean tryConnect = false;
    public void $startHB() {
        checkThread();
        String s = options.getMasterAddr();
        if ( master == null ) {
            if ( ! tryConnect ) {
                tryConnect = true;
                localLog.warn(this,"trying to connect "+s);
            }
            String[] split = s.split(":");
            try {
                TCPActorClient.Connect(
                    KollektivMaster.class,
                    split[0], Integer.parseInt(split[1]),
                    disconnectedRef -> self().$refDisconnected(s,disconnectedRef)
                )
                .onResult(actor -> {
                    master = actor;
                    localLog.Warn(this, "connection successful");
                    actor.$registerMember(new MemberDescription(self(), nodeId, options.getAvailableProcessors()))
                        .onResult(md -> {
                            masterDescription = md;
                            tryConnect = false;
                            Log.Lg.$setLogWrapper(
                                    (Thread t, int severity, Object source, Throwable ex, String msg) -> {
                                        String exString = null;
                                        if (ex != null) {
                                            StringWriter sw = new StringWriter();
                                            PrintWriter pw = new PrintWriter(sw);
                                            ex.printStackTrace(pw);
                                            exString = sw.toString();
                                        }
                                        if (actor.isStopped()) {
                                            if (Log.Lg.getSeverity() <= severity )
                                                Log.Lg.defaultLogger.msg(t, severity, source, ex, msg);
                                        } else {
                                            actor.$remoteLog(severity, nodeId + "[" + source + "]", exString == null ? msg : msg + "\n" + exString);
                                        }
                                    }
                            );
                            Log.Lg.warn(this, " start logging from " + nodeId);
                            delayed(1000, () -> self().$startHB());
                        });
                })
                .onError(err -> {
                    localLog.info(this,"failed to connect " + s);
                    delayed(RETRY_INTERVAL_MILLIS, () -> self().$startHB());
                });
            } catch (IOException e) {
                localLog.warn(this,"could not connect "+e);
            }
        } else {
            master.$heartbeat(nodeId);
            delayed(1000, () -> self().$startHB());
        }
    }

    public void $refDisconnected(String address, Actor disconnectedRef) {
        if ( disconnectedRef == master ) {
            Log.Lg.resetToSysout();
            master = null;
        }
        localLog.warn(this, "actor disconnected " + disconnectedRef + " address:" + address);
    }

    public static class ActorBootstrap {

        public Actor actor;

        public ActorBootstrap(Class actor) {
            this.actor = Actors.AsActor(actor);
        }

    }

    public Future<Actor> $run(String clazzname) {
        Promise res = new Promise();
        try {
            ActorAppBundle actorAppBundle = app;
            MemberClassLoader loader = actorAppBundle.getLoader();

            final RemoteConnection peek = master.__connections.peek();
            peek.setClassLoader(loader);

            Class<?> actorClazz = loader.loadClass(clazzname);
            Class<?> bootstrap = loader.loadClass(ActorBootstrap.class.getName());
            Object actorBS = bootstrap.getConstructor(Class.class).newInstance(actorClazz);
            Field f = actorBS.getClass().getField("actor");
            Actor resAct = (Actor) f.get(actorBS);
            actorAppBundle.addActor(resAct);
            res.receive(resAct,null);
        } catch (Exception e) {
            e.printStackTrace();
            res.receive(null,e);
        }
        return res;
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

    public Future $defineNameSpace( ActorAppBundle bundle ) {
        try {
            ActorAppBundle actorAppBundle = app;
            if ( actorAppBundle != null ) {
                actorAppBundle.getActors().forEach(actor -> actor.$stop());
            }
            File base = new File( options.getTmpDirectory() + File.separator + nodeId+File.separator+bundle.getName());
            int count = 0;
            while ( ! tryDelRecursive(base) ) {
                base = new File( base.getParent() + File.separator + bundle.getName()+"_"+ count++);
            }
            base.mkdirs();
            localLog.warn(this,"define name space " + bundle.getName() + " size " + bundle.getSizeKB()+" filebase:"+base.getAbsolutePath());
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
            MemberClassLoader memberClassLoader = new MemberClassLoader(bundle, new URL[]{base.toURL()}, getClass().getClassLoader());
            memberClassLoader.setBase( base );
            File[] list = base.listFiles();
            for (int i = 0; list != null && i < list.length; i++) {
                File file = list[i];
                if ( file.getName().endsWith(".jar") ) {
                    memberClassLoader.addURL(file);
                }
            }
            bundle.setLoader(memberClassLoader);
            app = bundle;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return new Promise<>(null);
    }

    public static class Options {
        @Parameter( names = {"-c","-cores"}, description = "specify how many cores should be available. (Defaults to number of cores reported by OS).")
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        @Parameter( names = {"-n", "-name"}, description = "optionally specify a symbolic name which can be used as a hint from the master process.")
        String name = "Node";
        @Parameter( names = { "-t", "-tmp"}, description = "specify temporary directory to deploy downloaded classes/jars. Defaults to /tmp")
        String tmpDirectory = "/tmp";
        @Parameter( names = {"-m", "-master"}, description = "define master addr:port. Defaults to 127.0.0.1:3456")
        String masterAddr = "127.0.0.1:3456";
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

    public static void main( String a[] ) {
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

        Log.Lg.$setSeverity(Log.WARN);
        System.out.println("starting kollektiv member with "+options );
        KollektivMember sl = Actors.AsActor(KollektivMember.class);
        sl.$init(options);
    }

}
