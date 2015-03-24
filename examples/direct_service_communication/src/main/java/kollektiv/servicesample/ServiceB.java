package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceB extends AbstractService<ServiceB> {

    @Override
    protected int getPort() {
        return 35001;
    }
}
