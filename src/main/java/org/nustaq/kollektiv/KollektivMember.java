package org.nustaq.kollektiv;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;
import org.nustaq.kontraktor.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.*;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMember extends Actor<KollektivMember> {

    public static int HB_MILLIS = 1000;
    public static final int DEFAULT_PORT = 3456;


    // messages passed to all hosted actors
    public static final String SHUTDOWN = "Shutdown"; // node will shutdown
    public static final String MASTER_LOST = "MasterLost"; // connection to master node lost

    KollektivMaster master;
    String nodeId;
    ActorAppBundle app;
    MasterDescription masterDescription;

    Options options;
    Log localLog;
//    List<Actor> actors = Collections.synchronizedList(new ArrayList<>());
    List<Actor> actors = new ArrayList<>();

    public void $init(Options options) {
        this.options = options;
        nodeId = options.getName()+"_"+MemberDescription.findHost()+"_"+Integer.toHexString((int)(System.currentTimeMillis()&0xffff));
        localLog = Actors.AsActor(Log.class,10000);
        localLog.$setSeverity(Log.WARN);
        $connectLoop();
    }

    boolean tryConnect = false;
    public void $connectLoop() {
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
                            if ( options.remoteLog ) {
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
                                             if (Log.Lg.getSeverity() <= severity)
                                                 Log.Lg.defaultLogger.msg(t, severity, source, ex, msg);
                                         } else {
                                             actor.$remoteLog(severity, nodeId + "[" + source + "]", exString == null ? msg : msg + "\n" + exString);
                                         }
                                     }
                                );
                                Log.Lg.warn(this, " start logging from " + nodeId);
                            }
                            MasterConnectedMsg message = new MasterConnectedMsg(master);
                            actors.forEach( act -> {
                                if ( ! act.isStopped() )
                                    act.$receive(message);
                            });
                        })
                        .onError( err -> {
                            tryConnect = false;
                            master = null;
                            actors.forEach( act -> act.$receive(MASTER_LOST) );
                            localLog.Warn(this, "registering failed "+err);
                        });
                })
                .onError(err -> {
                    localLog.info(this,"failed to connect " + s);
                });
            } catch (Exception e) {
                tryConnect = false;
                master = null;
                actors.forEach( actor -> actor.$receive(MASTER_LOST) );
                localLog.warn(this, "could not connect " + e);
            }
        } else {
            master.$heartbeat(nodeId);
        }
        delayed(HB_MILLIS, () -> self().$connectLoop());
    }

    public void $refDisconnected(String address, Actor disconnectedRef) {
        if ( disconnectedRef == master ) {
            Log.Lg.resetToSysout();
            master = null;
            actors.forEach( actor -> {
                if ( ! actor.isStopped() )
                    actor.$receive(MASTER_LOST);
            });
        }
        localLog.warn(this, "actor disconnected " + disconnectedRef + " address:" + address + " master: "+master );
    }

    public static class ActorBootstrap {

        public Actor actor;

        public ActorBootstrap(Class actor) {
            this.actor = Actors.AsActor(actor);
        }

    }

    public Future<Actor> $runMember(String clazzname) {
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
            addActor(resAct);
            res.complete(resAct, null);
        } catch (Exception e) {
            e.printStackTrace();
            res.complete(null, e);
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

    private void filterLiveActors() {
        actors = actors.stream().filter( actor -> !actor.isStopped() ).collect(Collectors.toList());
    }

    public void $sendToAllActors( Object message ) {
        filterLiveActors();
        actors.forEach(actor -> actor.$receive(message));
    }

    public void $shutdownAllActors() {
        filterLiveActors();
        $sendToAllActors(SHUTDOWN);
        actors.forEach( a -> a.$stop() );
    }

    public Future<List<Actor>> $allActors() {
        return new Promise<>(new ArrayList<>(actors));
    }

    public Future<List<ActorDescription>> $allActorNames() {
        filterLiveActors();
        return new Promise<>(actors.stream().map( in -> new ActorDescription(in) ).collect(Collectors.toList()) );
    }

    public Future $reconnect( KollektivMaster master ) {
        if ( app == null ) {
            return new Promise<>(null, new RuntimeException("member has no classdefinitions, but tries to reconnect."));
        }
        this.master = master;
        master.__connections.peek().setClassLoader(app.getLoader());
        MasterConnectedMsg msg = new MasterConnectedMsg(master);
        actors.forEach( act -> act.$receive(msg));
        return new Promise<>(null);
    }

    public Future $defineNameSpace( ActorAppBundle bundle ) {
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
            localLog.warn(this, "defined app bundle space " + bundle + " size " + bundle.getSizeKB() + " filebase:" + base.getAbsolutePath());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return new Promise<>(null);
    }

    public void $restart(long timeMillis) {
        localLog.warn(this, "restarting in "+timeMillis+" ms");
        self().$shutdownAllActors();
        StringBuilder cmd = new StringBuilder();
        cmd.append(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java ");
        for (String jvmArg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            cmd.append(jvmArg + " ");
        }
        cmd.append("-cp ").append(ManagementFactory.getRuntimeMXBean().getClassPath()).append(" ");
        cmd.append(KollektivMember.class.getName()).append(" ");
        for (String arg : args) {
            cmd.append(arg).append(" ");
        }
        delayed(timeMillis, () -> {
            try {
                Runtime.getRuntime().exec(cmd.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        });
    }

    public void $terminate(long timeMillis) {
        localLog.warn(this, "terminating in "+timeMillis+" ms");
        self().$shutdownAllActors();
        delayed(timeMillis, () -> {
            System.exit(0);
        });
    }

    private void addActor(Actor resAct) {
        actors.add(resAct);
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

        Log.Lg.$setSeverity(Log.WARN);
        System.out.println("===================================");
        System.out.println("==       kollektiv.MEMBER        ==");
        System.out.println("===================================");
        System.out.println("");
        System.out.println("starting kollektiv member with "+options );
        KollektivMember sl = Actors.AsActor(KollektivMember.class);
        sl.$init(options);
    }


}
