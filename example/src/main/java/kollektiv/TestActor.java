package kollektiv;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 07/03/15.
 */
public class TestActor extends Actor<TestActor> {

    public void $init() {
        System.out.println("init");
    }

    public Future<String> $method(String s) {
        System.out.println("method received "+s);
        return new Promise<>(s+s);
    }

    public Future<Long> $roundtrip( long time ) {
        return new Promise<>(time);
    }

    public static void main(String arg[]) throws Exception {

        KollektivMaster master = KollektivMaster.Start(3456);

        master.$onMemberMoreThan(1).then(() -> {
            System.out.println("starting remote actor");
            master.$run(TestActor.class, "Hello")
                .onError(e -> System.out.println(e))
                .onResult(act -> {
                    TestActor tact = (TestActor) act;
                    System.out.println(".. remote actor started: "+tact);
                    tact.$init();
                    tact.$method("Höö").onResult(res -> System.out.println(res));
                    new Thread( () -> {
                        int count = 0;
                        while( true && ! tact.isStopped() ) {
                            count++;
                            final int finalCount = count;
                            tact.$roundtrip(System.nanoTime()).onResult( l -> {
                                if ( finalCount%10000 == 0 ) {
                                    System.out.println(System.nanoTime() - l);
                                }
                            });
                            LockSupport.parkNanos(1000*100); // 1 ms
                        }
                    }).start();
                });
        });
    }

}
