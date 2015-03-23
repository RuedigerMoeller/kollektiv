package kollektiv.servicesample;

import org.nustaq.kontraktor.Actor;

import java.io.Serializable;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceDescription implements Serializable {

    String name;
    String host;
    int port;
    Class<? extends Actor> serviceClass;

    public ServiceDescription(String name, String host, int port, Class<? extends Actor> serviceClass) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.serviceClass = serviceClass;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Class<? extends Actor> getServiceClass() {
        return serviceClass;
    }
}
