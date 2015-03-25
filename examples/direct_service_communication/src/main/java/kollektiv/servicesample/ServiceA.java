package kollektiv.servicesample;

import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;
import org.nustaq.kontraktor.util.Log;

import java.io.IOException;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceA extends AbstractService<ServiceA> {

    ServiceB serviceB;

    @Override
    public void $init() {
        Log.Warn(this, "starting up A");
        if ( serviceMaster == null ) {
            delayed( 1000, () -> $init() );
        } else {
            tryConnect();
        }
    }

    protected void tryConnect() {
        // serviceA depends on service B.
        // create a direct connection to avoid tunneling each message  via ServiceMaster
        Log.Warn(this, "waiting for B");
        serviceMaster.$waitForService("ServiceB").onResult(serviceDesc -> {
            try {
                // note: there is a second variant of Connect which allows to install a failure handler
                Future<ServiceB> connect = TCPActorClient.Connect(ServiceB.class, serviceDesc.getHost(), serviceDesc.getPort());
                if (connect == null) {
                    // socket related error (still in use or refused) retry
                    delayed(500, () -> tryConnect());
                } else {
                    connect.then((serv, error) -> {
                        if (error != null) {
                            // connection established, but init errors, retry
                            delayed(500, () -> tryConnect());
                        } else {
                            serviceB = serv;
                            serviceEstablished();
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
                serviceB = null;
                delayed(500, () -> tryConnect());
            }
        });
    }

    private void serviceEstablished() {
        Log.Warn(this, "service established");
        serviceB.$helloFrom( "service A" ).then((r, e) -> {
            Log.Warn(this, "service B replied '" + r + "' error:" + e);
        });
    }

    public Future<String> $doubleMe(String s) {
        return new Promise<>(s+s);
    }

    @Override
    protected int getPort() {
        return 35000;
    }

}
