package kollektiv.datasample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.util.FutureLatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ruedi on 22/03/15.
 */
public class DataKontrolNode extends Actor<DataKontrolNode> {

    KollektivMaster master;
    List<DataMapActor> memberActors;

    public Future $main( String arg[] ) {
        try {
            master = KollektivMaster.Start( KollektivMember.DEFAULT_PORT, ConnectionType.Connect, self() );
            memberActors = new ArrayList<>();
            // we don't register for add/remove MemberNode events in this example
            // for sake of simplicity, just wait 5 seconds and continue with the fixed set
            // of cluster member nodes registered up to then (no dynamic cluster node handling in this example)
            delayed(5000, () -> {
                for (int i = 0; i < master.getMembers().size(); i++) {
                    MemberDescription mdesc = master.getMembers().get(i);
                    // await is like yield, but does not throw ex
                    DataMapActor node = master.$run(mdesc.getMember(), DataMapActor.class).await().get();
                    if ( node != null ) {
                        memberActors.add(node);
                    }
                }
                self().$runTestLogic();
            });
            return new Promise<>("success");
        } catch (Exception e) {
            return new Promise<>(null,e);
        }
    }

    public void $runTestLogic() {

        // as always: chaining plain sync logic with async actors is kind of a pain ...

        System.out.println("start running test logic");
        // fill in some data
        for ( int i = 0; i < 2000000; i++ ) {
            Object key = ""+i;
            Object value[] = { i, i*i, key };
            $put(key, value);
        }
        System.out.println("finished running test logic"); // actually its in flight ..

        Promise loopEnd = new Promise();
        FutureLatch latch = new FutureLatch( loopEnd, memberActors.size() );

        // send a spore to each DataMapActor
        memberActors.forEach( dmNode -> {
            dmNode.$doWithMap(new Spore<HashMap, Object>() {
                @Override
                public void remote(HashMap input) {
                    // could do arbitrary stuff on the local data of DataMapActor
                    // just stream back size of then
                    stream(input.size());
                }
            }
            .forEach((r, e) -> System.out.println("dmNode has size " + r))
            .onFinish( () -> latch.countDown() ));
        });

        // once spores finished, do some get ops
        loopEnd.then( () -> {
            $get("13").onResult(r1 -> System.out.println("13 -> " + Arrays.toString((Object[]) r1)));
            $get("14").onResult(r2 -> System.out.println("14 -> " + Arrays.toString((Object[]) r2)));
            $get("15").onResult(r2 -> System.out.println("14 -> " + Arrays.toString((Object[]) r2)));
            $get("16").onResult(r2 -> System.out.println("14 -> " + Arrays.toString((Object[]) r2)));
        });

    }

    /**
     * put key and value to remote nodes. keys are distributed evenly amongst
     * nodes using modulo of hashkey
     * @param key
     * @param value
     */
    public void $put(Object key, Object[] value) {
        int nodeNum = Math.abs(key.hashCode() % memberActors.size());
        DataMapActor dataMapActor = memberActors.get(nodeNum);
        dataMapActor.$put(key,value);
    }

    /**
     * get key from remote node. keys are distributed evenly amongst
     * nodes using modulo of hashkey
     * @param key
     */
    public Future $get(Object key) {
        int nodeNum = key.hashCode() % memberActors.size();
        DataMapActor dataMapActor = memberActors.get(nodeNum);
        return dataMapActor.$get(key);
    }

    public static void main( String a[] ) {
        AsActor(DataKontrolNode.class).$main(a).onError( error -> {
            if ( error instanceof Throwable )
                ((Throwable) error).printStackTrace();
            else
                System.out.println("oops ..");
            System.exit(0);
        });
    }

}
