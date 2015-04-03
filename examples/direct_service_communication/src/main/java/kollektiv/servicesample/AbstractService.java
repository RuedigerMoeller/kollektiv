package kollektiv.servicesample;

import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.IPromise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by ruedi on 24.03.2015.
 */
public abstract class AbstractService<T extends AbstractService> extends Actor<T> {

    protected ServiceMaster serviceMaster;
    protected TCPActorServer publisher;

    public void $init() {
    }

    @Override
    public IPromise $receive(Object message) {
        if (message instanceof InitMsg) {
            InitMsg initmsg = (InitMsg) message;
            serviceMaster = initmsg.getMaster();
            // publish the service and send description (incl addres:port) to master
            try {
                int port = getPort();
                publisher = TCPActorServer.Publish(self(), port);
                serviceMaster.$registerService(
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
        self().$init();
        return super.$receive(message);
    }

    abstract protected int getPort();

}
