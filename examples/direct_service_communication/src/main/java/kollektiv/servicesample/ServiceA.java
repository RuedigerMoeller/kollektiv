package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceA extends AbstractService<ServiceA> {

    public void $serviceAMethod() {
        System.out.println("$serviceAMethod called");
    }

    @Override
    protected int getPort() {
        return 35000;
    }
}
