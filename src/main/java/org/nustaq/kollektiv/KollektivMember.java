package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.beust.jcommander.*;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMember extends Actor<KollektivMember> {

    KollektivMaster master;
    String nodeId = "SL"+System.currentTimeMillis()+":"+(System.nanoTime()&0xffff);
    HashMap<String, ActorAppBundle> apps = new HashMap<>();
    MasterDescription masterDescription;

    Options options;

    public void $init(Options options) {
        this.options = options;
        $startHB();
    }

    public void $startHB() {
        checkThread();
        String s = options.getMasterAddr();
        if ( master == null ) {
            String[] split = s.split(":");
            try {
                TCPActorClient.Connect(
                    KollektivMaster.class,
                    split[0], Integer.parseInt(split[1]),
                    disconnectedRef -> self().$refDisconnected(s,disconnectedRef)
                )
                .onResult( actor -> {
                    master = actor;
                    actor.$registerMember(new MemberDescription( self(), nodeId, options.getAvailableProcessors() ))
                        .onResult( md -> {
                            masterDescription = md;
                            Log.Lg.$setLogWrapper(
                                (Thread t, int severity, Object source, Throwable ex, String msg) -> {
                                    String exString = null;
                                    if ( ex != null ) {
                                        StringWriter sw = new StringWriter();
                                        PrintWriter pw = new PrintWriter(sw);
                                        ex.printStackTrace(pw);
                                        exString = sw.toString();
                                    }
                                    actor.$remoteLog( severity, nodeId+":"+source, exString == null ? msg : msg + "\n" + exString );
                                }
                            );
                            Log.Lg.info(this, " start logging from "+nodeId );
                        });
                })
                .onError(err -> System.out.println("failed to connect " + s));
            } catch (IOException e) {
                System.out.println("could not connect "+e);
            }
        } else {
            master.$heartbeat(nodeId);
        }
        delayed( 1000, () -> self().$startHB() );
    }

    public void $refDisconnected(String address, Actor disconnectedRef) {
        if ( disconnectedRef == master ) {
            Log.Lg.$resetToSysout();
            master = null;
        }
        System.out.println("actor disconnected " + disconnectedRef + " address:" + address);
    }

    public static class ActorBootstrap {

        public Actor actor;

        public ActorBootstrap(Class actor) {
            this.actor = Actors.AsActor(actor);
        }

    }

    public Future<Actor> $run(String clazzname, String nameSpace) {
        Promise res = new Promise();
        try {
            ActorAppBundle actorAppBundle = apps.get(nameSpace);
            MemberClassLoader loader = actorAppBundle.getLoader();
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        base.delete();
        return !base.exists();
    }

    public Future $defineNameSpace( ActorAppBundle bundle ) {
        try {
            ActorAppBundle actorAppBundle = apps.get(bundle.getName());
            if ( actorAppBundle != null ) {
                actorAppBundle.getActors().forEach(actor -> actor.$stop());
            }
            File base = new File( options.getTmpDirectory() + File.separator + bundle.getName());
            int count = 0;
            while ( ! tryDelRecursive(base) ) {
                base = new File( options.getTmpDirectory() + File.separator + bundle.getName() + count++);
            }
            base.mkdirs();
            System.out.println("define name space " + bundle.getName() + " size " + bundle.getSizeKB()+" filebase:"+base.getAbsolutePath());
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
            apps.put(bundle.getName(),bundle);
            System.out.println(".. done");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return new Promise<>(null);
    }

    public static class Options {
        @Parameter
        int availableProcessors;
        @Parameter
        String name;
        @Parameter
        String tmpDirectory;
        @Parameter
        String masterAddr = "127.0.0.1:3456";

        public int getAvailableProcessors() {
            if (availableProcessors == 0 ) {
                return Runtime.getRuntime().availableProcessors();
            }
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

    }

    public static void main( String a[] ) {
        Options options = new Options();
        JCommander com = new JCommander(options,a);
        KollektivMember sl = Actors.AsActor(KollektivMember.class);
        sl.$init(options);
        sl.$refDisconnected(null,null);
    }

}
