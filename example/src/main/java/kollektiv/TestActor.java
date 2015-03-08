package kollektiv;

import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

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

    public static void main(String arg[]) throws Exception {

        KollektivMaster master = KollektivMaster.Start(3456);

        master.$onMemberMoreThan(1).then(() -> {
            master.$run(TestActor.class, "Hello")
                .onError(e -> System.out.println(e))
                .onResult(act -> {
                    TestActor tact = (TestActor) act;
                    System.out.println(tact);
                    tact.$init();
                    tact.$method("Huhu Hoho").onResult(res -> System.out.println(res));
                });
        });
    }

}
