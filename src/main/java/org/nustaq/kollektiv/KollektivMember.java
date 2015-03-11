package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMember extends Actor<KollektivMember> {

    String tmpDir = "/tmp";
    List<String> masterAddresses;
    HashMap<String,KollektivMaster> masters;
    String nodeId = "SL"+System.currentTimeMillis()+":"+(System.nanoTime()&0xffff);
    HashMap<String, ActorAppBundle> apps = new HashMap<>();


    public void $init() {
        List<String> addr = new ArrayList<>();
        addr.add("127.0.0.1:3456");
        $initWithOptions(addr);
    }

    public void $initWithOptions(List<String> masterAddresses) {
        this.masterAddresses = masterAddresses;
        masters = new HashMap<>();
        $startHB();
    }

    public void $startHB() {
        checkThread();
        for (int i = 0; i < masterAddresses.size(); i++) {
            String s = masterAddresses.get(i);
            if ( masters.get(s) == null ) {
                String[] split = s.split(":");
                try {
                    TCPActorClient.Connect(
                        KollektivMaster.class,
                        split[0], Integer.parseInt(split[1]),
                        disconnectedRef -> self().$refDisconnected(s,disconnectedRef)
                    )
                    .onResult(actor -> {
                        masters.put(s, actor);
                        actor.$registerMember(new MemberDescription(self(),nodeId,-1));
                    })
                    .onError(err -> System.out.println("failed to connect " + s));
                } catch (IOException e) {
                    System.out.println("could not connect "+e);
                }
            }
        }
        delayed(1000, () -> self().$startHB());
    }

    public void $refDisconnected(String address, Actor disconnectedRef) {
        checkThread();
        System.out.println("actor disconnected "+disconnectedRef+" address:"+address);
        masters.remove(address);
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
            File base = new File(tmpDir + File.separator + bundle.getName());
            int count = 0;
            while ( ! tryDelRecursive(base) ) {
                base = new File(tmpDir + File.separator + bundle.getName() + count++);
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

    public static void main( String a[] ) {
        KollektivMember sl = Actors.AsActor(KollektivMember.class);
        sl.$init();
        sl.$refDisconnected(null,null);
    }

}
