package kollektiv.datasample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
            delayed(5000, () -> {
                // for sake of simplicity, just wait 5 seconds and just continue with the fixed set
                // of cluster member nodes registered up to then (no dynamic cluster node handling in this example)
                Actors.yield(
                    master.getMembers().stream()
                        .map(memberDescription -> master.$runOnDescription(memberDescription, DataMapNode.class))
                        .collect(Collectors.toList())
                ).onResult( list -> list.forEach( fut -> memberActors.add((DataMapNode) fut.getResult()) ) );
            });
            return new Promise<>("success");
        } catch (Exception e) {
            return new Promise<>(null,e);
        }
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
