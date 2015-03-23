package kollektiv.datasample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kontraktor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ruedi on 22/03/15.
 */
public class DataKontrolNode extends Actor<DataKontrolNode> {

    KollektivMaster master;
    List<DataMapNode> memberActors;

    public Future $main( String arg[] ) {
        try {
            master = KollektivMaster.Start( KollektivMember.DEFAULT_PORT, ConnectionType.Connect, self() );
            memberActors = new ArrayList<>();
            // we don't register for add/remove MemberNode events in this example
            delayed( 5000, () -> {
                // for sake of simplicity, just wait 5 seconds and continue with the fixed set
                // of cluster member nodes registered up to then (no dynamic cluster node handling in this example)
                Actors.yieldEach(
                    master.getMembers().stream()
                        .map( memberDescription -> master.$run(memberDescription.getMember(), DataMapNode.class) ),
                    (res,err) -> memberActors.add(res)
                ).onResult( (dummy) -> self().$runTestLogic() );
            });
            return new Promise<>("success");
        } catch (Exception e) {
            return new Promise<>(null,e);
        }
    }

    public void $runTestLogic() {

        System.out.println("start running test logic");
        // fill in some data
        for ( int i = 0; i < 2000000; i++ ) {
            Object key = ""+i;
            Object value[] = { i, i*i, key };
            $put(key, value);
        }
        System.out.println("finished running test logic");

        memberActors.forEach( dmNode -> {
            dmNode.$onMap(new Spore<HashMap, Object>() {
                @Override
                public void remote(HashMap input) {
                    returnResult(input.size());
                }
            }.then( (r,e) -> {
                System.out.println("dmNode has size "+r);
            })).then( () -> {
                $get("13").onResult(r1 -> System.out.println("13 -> "+ Arrays.toString((Object[])r1)));
                $get("14").onResult(r2 -> System.out.println("14 -> "+ Arrays.toString((Object[])r2)));
            });
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
        DataMapNode dataMapNode = memberActors.get(nodeNum);
        dataMapNode.$put(key,value);
    }

    /**
     * get key from remote node. keys are distributed evenly amongst
     * nodes using modulo of hashkey
     * @param key
     */
    public Future $get(Object key) {
        int nodeNum = key.hashCode() % memberActors.size();
        DataMapNode dataMapNode = memberActors.get(nodeNum);
        return dataMapNode.$get(key);
    }

    public static void main( String a[] ) {
        Actors.AsActor(DataKontrolNode.class).$main(a).onError( error -> {
            if ( error instanceof Throwable )
                ((Throwable) error).printStackTrace();
            else
                System.out.println("oops ..");
            System.exit(0);
        });
    }

}
