package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;
import org.nustaq.kontraktor.util.Log;

import java.io.IOException;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceC extends AbstractService<ServiceC> {

    ServiceB serviceB;
    ServiceA serviceA;

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
        // serviceC depends on service A and B.
        // create a direct connections to avoid tunneling each message  via ServiceMaster
        Log.Warn(this, "waiting for B");
        Future<Future<ServiceDescription>[]> bothpresent = yield(
            serviceMaster.$waitForService("ServiceA"),
            serviceMaster.$waitForService("ServiceB")
        );

        bothpresent.onResult(futArr -> {
            for (int i = 0; i < futArr.length; i++) {
                int finalI = i;
                ServiceDescription serviceDesc = futArr[i].getResult();
                Class<? extends Actor> clazz = i==0 ? ServiceA.class : ServiceB.class;

                try {
                    // note: there is a second variant of Connect which allows to install a failure handler
                    Future<? extends Actor> connect = TCPActorClient.Connect(clazz, serviceDesc.getHost(), serviceDesc.getPort());
                    if (connect == null) {
                        // socket related error (still in use or refused) retry
                        delayed(500, () -> tryConnect());
                    } else {
                        connect.then((serv, error) -> {
                            if (error != null) {
                                // connection established, but init errors, retry
                                delayed(500, () -> tryConnect());
                            } else {
                                if ( finalI == 0 )
                                    serviceA = (ServiceA) serv;
                                else
                                    serviceB = (ServiceB) serv;
                                serviceEstablished();
                            }
                        });
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    serviceB = null;
                    delayed(500, () -> tryConnect());
                }
            }
        });
    }

    private void serviceEstablished() {
        Log.Warn(this, "service established");
        serviceB.$helloFrom( "service C" ).then((r, e) -> {
            Log.Warn(this, "service B replied '" + r + "' error:" + e);
        });
        serviceA.$doubleMe( "from service C <" ).then( (r, e) -> {
            Log.Warn(this,"service A replied '" + r + "' error:" + e);
        });
    }



    @Override
    protected int getPort() {
        return 35002;
    }
}
