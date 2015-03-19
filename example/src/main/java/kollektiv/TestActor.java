package kollektiv;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;
import org.nustaq.kontraktor.util.Log;

import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 07/03/15.
 */
public class TestActor extends Actor<TestActor> {

    KollektivMaster master;

    public void $init(KollektivMaster master) {
        this.master = master;
        delayed( 2000, () -> self().$showMembers() );
    }

    public Future<String> $method(String s) {
        Log.Info( this, "method received "+s);
        return new Promise<>(s+s);
    }

    public Future<Long> $roundtrip( long time ) {
        return new Promise<>(time);
    }

    public void $showMembers() {
        master.$getMembers( (r,e) -> {
            Log.Lg.info(this, "" + r);
        });
        delayed(2000, () -> self().$showMembers());
    }

    public static void main(String arg[]) throws Exception {

        KollektivMaster master = KollektivMaster.Start(8080);

        master.$onMemberAdd(description -> {
            master.$run(TestActor.class, "Hello")
                .onResult(testAct -> {
                    testAct.$method("ei ei from "+description.getNodeId()).onResult(r -> System.out.println(r)).onError( e -> System.out.println("ERROR "+e));
                    for (int i = 0; i < 10; i++) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        testAct.$roundtrip(System.currentTimeMillis()).onResult( r -> System.out.println("from "+description.getNodeId()+" "+(System.currentTimeMillis()-r)));
                    }
                })
                .onError(err -> { System.out.println("error during start "+err+" from "+description.getNodeId());
                    if ( err instanceof Throwable )
                        ((Throwable) err).printStackTrace();
                });
            return false;
        });
    }

}
