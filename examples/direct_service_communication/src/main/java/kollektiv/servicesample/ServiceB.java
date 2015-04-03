package kollektiv.servicesample;

import org.nustaq.kontraktor.IPromise;
import org.nustaq.kontraktor.Promise;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceB extends AbstractService<ServiceB> {

    @Override
    protected int getPort() {
        return 35001;
    }

    public IPromise<Object> $helloFrom(String s) {
        return new Promise( "B received '"+s+"'");
    }

}
