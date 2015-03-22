package kollektiv;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.impl.DispatcherThread;
import org.nustaq.kontraktor.util.Log;

import java.util.HashMap;
import java.util.List;

/**
 * Created by ruedi on 07/03/15.
 */
public class TestActor extends Actor<TestActor> {

    KollektivMaster master;
    HashMap aMap = new HashMap();

    public void $init(KollektivMaster master) {
        this.master = master;
    }

    public Future<String> $method(String s) {
        Log.Info( this, "method received "+s);
        return new Promise<>(s+s);
    }

    public Future<Long> $roundtrip( long time ) {
        return new Promise<>(time);
    }

    public void $onMap( Spore<HashMap,Object> spore ) {
        spore.remote(aMap);
    }

    public void $showMembers(int count) {
        master.$getMembers( (r,e) -> {
            Log.Info(this, "" + r);
        });
        if ( count > 0 )
            delayed(2000, () -> self().$showMembers(count-1));
    }

    static void runStuff(MemberDescription description, TestActor testAct) {
        testAct.$method("ei ei from " + description.getNodeId()).onResult(r -> System.out.println(r)).onError(e -> System.out.println("ERROR " + e));
        for (int i = 0; i < 10; i++) {
            testAct.$roundtrip(System.currentTimeMillis()).onResult(r -> System.out.println("from " + description.getNodeId() + " " + (System.currentTimeMillis() - r)));
        }
        testAct.$onMap(new Spore<HashMap, Object>() {
                @Override
                public void remote(HashMap input) {
                    for (int i = 0; i < 1000000; i++) {
                        input.put(input.size(), "str " + input.size());
                    }
                    returnResult(input.size(), null);
                }
            }.then((res, err) -> System.out.println("Spore returned " + res))
        );
        testAct.$onMap(new Spore<HashMap, Object>() {
                @Override
                public void remote(HashMap input) {
                    for (int i = 0; i < 10; i++) {
                        stream(input.get(i));
                    }
                    finished();
                }
            }.then((res, err) -> System.out.println("Spore streamed " + res))
        );
    }

    public static void main(String arg[]) throws Exception {

        DispatcherThread.DUMP_CATCHED = true; // ease diagnostics

        ConnectionType conT = ConnectionType.Reconnect;
//        ConnectionType conT = ConnectionType.Connect;

        KollektivMaster master = KollektivMaster.Start(3456,conT);

        if ( conT == ConnectionType.Connect ) {
            master.$onMemberAdd(description -> {
                master.$run(description.getMember(),TestActor.class)
                    .onResult( testAct -> {
                        testAct.$init(master);
                        runStuff(description, testAct);
                    })
                    .onError(err -> {
                        System.out.println("error during start " + err + " from " + description.getNodeId());
                        if (err instanceof Throwable)
                            ((Throwable) err).printStackTrace();
                    });
                return false;
            });
        } else {
            master.$onMemberAdd(description -> {
                List<Actor> remotedActors = description.getRemotedActors();
                if ( remotedActors.size() == 0 ) {
                    master.$run(description.getMember(),TestActor.class)
                        .onResult( testAct -> {
                            testAct.$init(master);
                            runStuff(description, testAct);
                        })
                        .onError(err -> {
                            System.out.println("error during start " + err + " from " + description.getNodeId());
                            if (err instanceof Throwable)
                                ((Throwable) err).printStackTrace();
                        });
                } else {
                    remotedActors.forEach(actor -> {
                        if (actor instanceof TestActor) {
                            TestActor testAct = (TestActor) actor;
                            runStuff(description, testAct);
                        }
                    });
                }
                return false;
            });
        }
    }

}
