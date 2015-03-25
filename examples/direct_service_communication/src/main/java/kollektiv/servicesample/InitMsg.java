package kollektiv.servicesample;

import java.io.Serializable;

/**
 * Created by ruedi on 23/03/15.
 *
 * Init message delivered for init to all services via untyped $settle method.
 */
public class InitMsg implements Serializable {

    ServiceMaster smaster; // just send a remoteref to masternode

    public InitMsg(ServiceMaster smaster) {
        this.smaster = smaster;
    }

    public ServiceMaster getMaster() {
        return smaster;
    }
}
