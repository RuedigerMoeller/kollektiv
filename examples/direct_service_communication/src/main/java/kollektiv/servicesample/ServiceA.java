package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceA extends Actor<ServiceA> {

    ServiceMaster sm;

    @Override
    public Future $receive(Object message) {
        if ( message instanceof InitMsg ) {
            InitMsg initmsg = (InitMsg) message;
            sm = initmsg.getMaster();
            // publish the service and send description (incl addres:port) to master
            try {
                int port = 35000;
                TCPActorServer.Publish(self(), port);
                sm.$registerService(
                    new ServiceDescription(
                        "THE_A",
                        InetAddress.getLocalHost().getHostName(),
                        port,
                        ServiceA.class
                    )
                );
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return super.$receive(message);
    }
}
