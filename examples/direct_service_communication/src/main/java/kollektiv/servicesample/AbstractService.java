package kollektiv.servicesample;

import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by ruedi on 24.03.2015.
 */
public abstract class AbstractService<T extends Actor> extends Actor<T> {

    protected ServiceMaster sm;
    protected TCPActorServer publisher;

    @Override
    public Future $receive(Object message) {
        if (message instanceof InitMsg) {
            InitMsg initmsg = (InitMsg) message;
            sm = initmsg.getMaster();
            // publish the service and send description (incl addres:port) to master
            try {
                int port = getPort();
                publisher = TCPActorServer.Publish(self(), port);
                sm.$registerService(
                        new ServiceDescription(
                                getClass().getSimpleName(),
                                InetAddress.getLocalHost().getHostName(),
                                port,
                                getClass()
                        )
                );
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (KollektivMember.SHUTDOWN.equals(message)) {
            publisher.closeConnection();
        }
        return super.$receive(message);
    }

    abstract protected int getPort();

}
