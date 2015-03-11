package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMaster extends Actor<KollektivMaster> {

    public static KollektivMaster Start( int port ) throws Exception {
        KollektivMaster master = Actors.AsActor(KollektivMaster.class);
        master.$init();
        TCPActorServer.Publish(master, port, closedActor -> master.$memberDisconnected(closedActor));
        return master;
    }

    ActorAppBundle cachedBundle;
    String cachedCP;

    static class ListTrigger {
        public ListTrigger(Supplier<Boolean> condition, Future toNotify) {
            this.condition = condition;
            this.toNotify = toNotify;
        }

        Supplier<Boolean> condition;
        Future toNotify;
    }

    List<ListTrigger> triggers = new ArrayList<>();

    // shared state
    Map<String,MemberDescription> memberMap = new ConcurrentHashMap<>();
    Map<String,MemberDescription> roleMap = new ConcurrentHashMap<>();
    List<MemberDescription> members;
    //..

    public void $init() {
        members = new ArrayList<>();
    }

    public void $registerMember(MemberDescription sld) {
        System.out.println("receive registration " + sld + " members:" + members.size() + 1);
        addMember(sld);
        sld.getMember().$defineNameSpace(getCachedBundle(sld.getClasspath())).then( (r,e) -> {
            if ( e == null )
                System.out.println("transfer COMPLETE: "+sld);
            else {
                if ( e instanceof Throwable ) {
                    ((Throwable)e).printStackTrace();
                }
                System.out.println("transfer FAILED: " + sld + " " + e);
            }
        });
    }

    void addMember(MemberDescription sld) {
        synchronized (members) {
            members.add(sld);
            memberMap.put(sld.getNodeId(), sld );
        }
    }

    boolean removeMember(Actor closedActor) {
        boolean res = members.remove(closedActor);
        for (Iterator<Map.Entry<String, MemberDescription>> iterator = memberMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, MemberDescription> next = iterator.next();
            if ( next.getValue().getMember() == closedActor )
            {
                memberMap.remove(next.getKey());
                break;
            }
        }
        for (Iterator<Map.Entry<String, MemberDescription>> iterator = roleMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, MemberDescription> next = iterator.next();
            if ( next.getValue().getMember() == closedActor )
            {
                roleMap.remove(next.getKey());
                break;
            }
        }
        return res;
    }


    ActorAppBundle getCachedBundle(String classpath) {
        if ( cachedBundle == null ) {
            cachedCP = classpath;
            cachedBundle = new ActorAppBundle("Hello");
            buildAppBundle(cachedBundle,classpath);
        } else {
            // some members might have a different set of predefined jars ..
            if ( ! cachedCP.equals(classpath) ) {
                ActorAppBundle bundle = new ActorAppBundle("Hello");
                buildAppBundle(bundle,classpath);
                return bundle;
            }
        }
        return cachedBundle;
    }

    void buildAppBundle(ActorAppBundle bundle, String classpath) {
        String[] foreignpath = classpath.split(":;:");
        HashSet<String> presentjars = new HashSet<>();
        for (int i = 0; i < foreignpath.length; i++) {
            String s = foreignpath[i];
            if ( s.endsWith(".jar") )
                presentjars.add( new File(s).getName());
        }
        String cp = System.getProperty("java.class.path");
        String bcp = System.getProperty("sun.boot.class.path");
        String[] path = cp.split(File.pathSeparator);
        Set<String> bootCP = Arrays.stream(bcp.split(File.pathSeparator)).collect(Collectors.toSet());
        for (int i = 0; i < path.length; i++) {
            String s = path[i];
            if ( ! bootCP.contains(s) &&
                 s.indexOf("jre"+File.separator+"lib"+File.separator) < 0
               )
            {
                File pathOrJar = new File(s);
                if (s.endsWith(".jar") && pathOrJar.exists() && ! presentjars.contains(pathOrJar.getName()) ) {
                    try {
                        bundle.put(pathOrJar.getName(), Files.readAllBytes(Paths.get(s)) );
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else if (pathOrJar.isDirectory() && pathOrJar.exists()) {
                    try {
                        Files.walk(Paths.get(s), 65536).forEach(p -> {
                            File file = p.toAbsolutePath().toFile();
                            if (!file.isDirectory()) {
                                try {
                                    String rel = p.toString();
                                    rel = rel.substring(s.length(),rel.length());
                                    rel.replace(File.separatorChar,'/');
                                    bundle.put( rel, Files.readAllBytes(p));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("ignore " + s);
                }
            }
        }
    }

    public void $memberDisconnected(Actor closedActor) {
        removeMember(closedActor);
        System.out.println("member disconnected "+closedActor+" members remaining:"+members.size());
    }

    public Future $onMemberMoreThan(int i) {
        Promise p = new Promise();
        triggers.add(new ListTrigger(() -> members.size() >= i, p));
        evaluateTriggers();
        return p;
    }

    private void evaluateTriggers() {
        triggers = triggers.stream().filter(trigger -> {
            if (trigger.condition.get().booleanValue()) {
                trigger.toNotify.signal();
                return false;
            }
            return true;
        }).collect( Collectors.toList() );
    }

    public Future<Actor> $run(Class actorClass, String nameSpace) {
        if ( members.size() == 0 ) {
            return new Promise<>(null,"no members avaiable");
        }
        Promise res = new Promise<>();
        members.get(0).getMember().$run(actorClass.getName(), nameSpace).then((r, e) -> {
            res.receive(r, e);
        });
        return res;
    }

    public Future<List<MemberDescription>> $getMembers() {
        return new Promise<>(null);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  sync API
    //

    @CallerSideMethod public void setRole( KollektivMember member, String role ) {
        List<MemberDescription> copy = getActor().members;
        synchronized (copy) {
            for (int i = 0; i < copy.size(); i++) {
                MemberDescription kollektivMember = copy.get(i);
                if ( kollektivMember.getMember() == member ) {
                    getActor().roleMap.put(role,kollektivMember);
                }
            }
        }
    }

    @CallerSideMethod public KollektivMember byRole( String role ) {
        MemberDescription memberDescription = getActor().roleMap.get(role);
        if ( memberDescription != null )
            return memberDescription.getMember();
        return null;
    }

    //fixme: slowish
    @CallerSideMethod public MemberDescription getDescription( KollektivMember ref ) {
        for (int i = 0; i < members.size(); i++) {
            MemberDescription memberDescription = members.get(i);
            if ( memberDescription.getMember() == ref ) {
                return memberDescription;
            }
        }
        return null;
    }

    @CallerSideMethod public KollektivMember byId( String nodeId ) {
        MemberDescription memberDescription = getActor().memberMap.get(nodeId);
        if ( memberDescription != null )
            return memberDescription.getMember();
        return null;
    }

    @CallerSideMethod public List<MemberDescription> getMembers() {
        synchronized (getActor().members) {
            return new ArrayList<>(members);
        }
    }

}
