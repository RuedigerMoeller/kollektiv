package kollektiv.servicesample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;

import java.util.*;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceMaster extends Actor<ServiceMaster> {

    static class ListenerEntry {
        String serviceName;
        Promise toNotify;

        public ListenerEntry(String serviceName, Promise toNotify) {
            this.serviceName = serviceName;
            this.toNotify = toNotify;
        }
    }

    HashMap<String,List<ServiceDescription>> services = new HashMap<>();
    ArrayList<ListenerEntry> clients = new ArrayList<>();

    public void $registerService( ServiceDescription desc ) {
        System.out.println("register service '"+desc.getName()+"'at "+desc.getHost()+":"+desc.getPort());
        List<ServiceDescription> serviceDescriptions = services.get(desc.getName());
        if ( serviceDescriptions == null ) {
            serviceDescriptions = new ArrayList<>();
            services.put(desc.getName(),serviceDescriptions);
        }
        serviceDescriptions.add(desc);
        // check if someone has been waiting for this service and notify in case
        for (int i = 0; i < clients.size(); i++) {
            ListenerEntry listenerEntry = clients.get(i);
            if ( listenerEntry.serviceName.equals(desc.getName() ) ) {
                listenerEntry.toNotify.settle(desc, null);
                clients.remove(i);
                i--;
            }
        }
    }

    public Future<ServiceDescription> $waitForService( String name ) {
        Promise<ServiceDescription> res = new Promise<>();
        ServiceDescription desc = findService(name);
        if ( desc != null ) {
            res.settle(desc, null);
        } else {
            // if service is not present, add it to list and
            // fulfil the future once the required service registers
            clients.add(new ListenerEntry(name,res));
        }
        return res;
    }

    private ServiceDescription findService(String name) {
        // generics are quite verbose at time ;) ..
        for (Iterator<Map.Entry<String, List<ServiceDescription>>> iterator = services.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, List<ServiceDescription>> next = iterator.next();
            if ( next.getKey().equals(name) && next.getValue().size() > 0 ) {
                return next.getValue().get(0);
            }
        }
        return null;
    }

    public void $main(String a[]) {
        List<Class<? extends Actor>> servicesToStart = new ArrayList<>(Arrays.asList(
            ServiceC.class,
            ServiceB.class,
            ServiceA.class
        ));
        try {
            KollektivMaster master = KollektivMaster.Start(KollektivMember.DEFAULT_PORT, ConnectionType.Connect, self());
            Promise allNodesStarted = new Promise<>();
            master.$onMemberAdd( memberDesc -> {
                if ( servicesToStart.size() > 0 ) {
                    Class<? extends Actor> toStart = servicesToStart.remove(servicesToStart.size() - 1);
                    final int toStarteSize = servicesToStart.size(); // caveat: need to capture
                    master.$run( memberDesc.getMember(), toStart).then( (remoteRef,error) -> {
                        if ( error != null ) {
                            servicesToStart.add(toStart);
                            System.out.println("Failed to start actor");
                        } else {
                            //System.out.println("member added "+memberDesc+" tostart:"+servicesToStart.size());
                            remoteRef.$receive( new InitMsg(self()) );
                            if ( toStarteSize == 0 ) {
                                allNodesStarted.settle();
                            }
                            // registering is done by each service on its own.
                            //$registerService(new ServiceDescription());
                        }
                    });

                }
            });
            master.$onMemberRem(memberDesc -> {
                // not implemented in example
            });
            allNodesStarted.then(() -> {
                System.out.println("All nodes started");
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void main(String a[]) {
        AsActor(ServiceMaster.class).$main(a);
    }

}
