package org.nustaq.kollektiv;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.annotations.CallerSideMethod;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;
import org.nustaq.kontraktor.util.Log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by moelrue on 3/6/15.
 */
public class KollektivMaster extends Actor<KollektivMaster> {

    public static KollektivMaster Start( int port, ConnectionType type, Actor appProvided ) throws Exception {
        KollektivMaster master = Actors.AsActor(KollektivMaster.class);
        master.$init(type,appProvided);
        TCPActorServer.Publish(master, port, closedActor -> master.$memberDisconnected(closedActor));
        return master;
    }

    ActorAppBundle cachedBundle;
    String cachedCP;

    public void $heartbeat(String id) {
        MemberDescription memberDescription = memberMap.get(id);
        if ( memberDescription != null )
            memberDescription.updateHeartbeat();
        else {
            // System.out.println("hearbeat of unknown member "+id); avoid false alarm during init would be
        }
    }

    static class ListTrigger {
        public static int ADD = 1;
        public static int REM = 2;
        public static int LIST_RELATED = 3;

        public ListTrigger(Function<MemberDescription,Boolean> condition, int type) {
            this.setCondition(condition);
            this.setType(type);
        }

        private Function<MemberDescription,Boolean> condition;
        private int type;

        public Function<MemberDescription, Boolean> getCondition() {
            return condition;
        }

        public void setCondition(Function<MemberDescription, Boolean> condition) {
            this.condition = condition;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    List<ListTrigger> triggers = new ArrayList<>();
    ConnectionType connectionType;

    // shared state
    Map<String,MemberDescription> memberMap = new ConcurrentHashMap<>();
    List<MemberDescription> members;
    //..

    Actor customFacade; // application provided to allow talkback from member nodes to master

    public void $init(ConnectionType type, Actor appProvided) {
        members = Collections.synchronizedList(new ArrayList<MemberDescription>());
        this.connectionType = type;
        this.customFacade = appProvided;
    }

    public Future<MasterDescription> $registerMember(MemberDescription sld) {
        Log.Info(this, "receive registration " + sld + " members:" + members.size() + 1);
        Promise p = new Promise();
        if ( connectionType == ConnectionType.Reconnect ) {
            sld.getMember().$reconnect(self())
                .onError( err -> fullConnect(sld,p) )
                .onResult( dummy -> {
                    sld.getMember().$allActors().then((list, err) -> {
                        if (err == null) {
                            list.forEach(remoteactor -> sld.getRemotedActors().add(remoteactor));
                            addMember(sld);
                            p.receiveResult(new MasterDescription());
                        } else {
                            p.receiveError(err);
                        }
                    });
                });
        } else if ( connectionType == ConnectionType.Connect ) {
            fullConnect(sld, p);
        } else {
            sld.getMember().$reconnect(self());
            addMember(sld);
            p.receiveResult(new MasterDescription());
        }
        return p;
    }

    void fullConnect(MemberDescription sld, Promise p) {
        sld.getMember().$shutdownAllActors();
        sld.getMember().$defineNameSpace(getCachedBundle(sld.getClasspath())).then( (r, e) -> {
            if (e == null) {
                Log.Info(this, "transfer COMPLETE: " + sld);
                addMember(sld);
                p.receive( new MasterDescription(), null );
            }
            else {
                if (e instanceof Throwable) {
                    ((Throwable) e).printStackTrace();
                }
                Log.Info(this, "transfer FAILED: " + sld + " " + e);
                p.receive(null,e);
            }
        });
    }

    void addMember(MemberDescription sld) {
        members.add(sld);
        memberMap.put(sld.getNodeId(), sld );
        evaluateTriggers(ListTrigger.ADD, sld);
    }

    boolean removeMember(Actor closedActor) {
        boolean res = members.remove(closedActor);
        for (Iterator<Map.Entry<String, MemberDescription>> iterator = memberMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, MemberDescription> next = iterator.next();
            if ( next.getValue().getMember() == closedActor )
            {
                memberMap.remove(next.getKey());
                evaluateTriggers(ListTrigger.REM,next.getValue());
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
                        bundle.put(pathOrJar.getName(), Files.readAllBytes(Paths.get(s)));
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
                    Log.Info(this, "ignore " + s);
                }
            }
        }
    }

    public void $memberDisconnected(Actor closedActor) {
        removeMember(closedActor);
        Log.Info(this, "member disconnected " + closedActor + " members remaining:" + members.size());
    }

    /**
     * called on each member add until true is returned from given closure (true means unregister listener)
     * @param md
     */
    public void $onMemberAdd(Function<MemberDescription,Boolean> md) {
        triggers.add(new ListTrigger(description -> md.apply(description), ListTrigger.ADD));
        members.forEach(member -> evaluateTriggers( ListTrigger.ADD, member ));
    }

    public void $onMemberRem(Function<MemberDescription,Boolean> md) {
        triggers.add(new ListTrigger(description -> md.apply(description), ListTrigger.REM));
    }

    /**
     * fires once if number of members is >= given number
     * @param i
     * @return
     */
    public Future $onMemberMoreThan(int i) {
        Promise p = new Promise();
        triggers.add(
            new ListTrigger( (description) -> {
                if ( members.size() >= i) {
                    p.notify();
                    return true;
                }
                return false;
            }, ListTrigger.LIST_RELATED)
        );
        evaluateTriggers(ListTrigger.LIST_RELATED, null);
        return p;
    }

    private void evaluateTriggers(int actionType, MemberDescription item) {
        triggers = triggers.stream().filter(trigger -> {
            if ((trigger.getType() == actionType)) {
                if (trigger.getCondition().apply(item).booleanValue()) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
    }

    @CallerSideMethod public <T extends Actor> Future<T> $runOnDescription( MemberDescription description, Class<T> actorClass) {
        return $run( description.getMember(), actorClass );
    }

    public <T extends Actor> Future<T> $run( KollektivMember member, Class<T> actorClass) {
        if ( members.size() == 0 ) {
            return new Promise<>(null,"no members available");
        }
        Promise res = new Promise<>();
        member.$run(actorClass.getName()).then((r, e) -> {
            if ( r != null ) {
                member.$addActor(member);
            }
            res.receive(r, e);
        });
        return res;
    }

    public void $getMembers( Callback<MemberDescription> cb ) {
        members.forEach( member -> cb.receive(member,CONT) );
        cb.receive(null, FINSILENT);
    }

    public void $remoteLog( int severity, String source, String msg ) {
        Log.Lg.$msg( null, severity, source, null, msg );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  sync, local API
    //

    //fixme: slowish
    @CallerSideMethod public MemberDescription getDescription( KollektivMember ref ) {
        if ( isRemote() )
            throw new RuntimeException("cannot call on remote proxy");
        synchronized (getActor().getMembers()) {
            for (int i = 0; i < members.size(); i++) {
                MemberDescription memberDescription = members.get(i);
                if (memberDescription.getMember() == ref) {
                    return memberDescription;
                }
            }
        }
        return null;
    }

    @CallerSideMethod public KollektivMember byId( String nodeId ) {
        if ( isRemote() )
            throw new RuntimeException("cannot call on remote proxy");
        MemberDescription memberDescription = getActor().memberMap.get(nodeId);
        if ( memberDescription != null )
            return memberDescription.getMember();
        return null;
    }

    @CallerSideMethod public List<MemberDescription> getMembers() {
        if ( isRemote() )
            throw new RuntimeException("cannot call on remote proxy");
        return new ArrayList<>(getActor().members);
    }

}
