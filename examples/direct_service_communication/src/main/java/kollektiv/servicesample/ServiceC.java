package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceC extends AbstractService<ServiceC> {

    @Override
    protected int getPort() {
        return 35002;
    }
}
