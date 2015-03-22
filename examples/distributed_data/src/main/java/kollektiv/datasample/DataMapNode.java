package kollektiv.datasample;

import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kollektiv.MasterConnectedMsg;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.Spore;
import org.nustaq.kontraktor.util.Log;

import java.util.HashMap;

/**
 * Created by ruedi on 22/03/15.
 */
public class DataMapNode<K,V> extends Actor<DataMapNode<K,V>> {

    HashMap<K,V> mappedValues = new HashMap<K,V>();
    private DataKontrolNode kontrol;

    public void $init( DataKontrolNode kontrol ) {
        this.kontrol = kontrol;
    }

    public void $put( K key, V value ) {
        mappedValues.put(key,value);
    }

    public Future<V> $get( K key ) {
        return new Promise(mappedValues.get(key) );
    }

    public void $onMap( Spore<HashMap<K,V>, Object> spore ) {
        spore.remote(mappedValues);
        spore.finished(); // close channel
    }

    @Override
    public Future $receive(Object message) {
        if ( message == KollektivMember.MASTER_LOST ) {
            // null or flag references to master in case
            System.out.println("master lost msg");
        }
        if ( message == KollektivMember.SHUTDOWN ) {
            // cleanup
            System.out.println("shutdown msg");
        }
        if ( message instanceof MasterConnectedMsg ) {
            // refresh references to master in case
            System.out.println("master connected");
        }
        return new Promise<>("dummy"); // just signal finish
    }
}
