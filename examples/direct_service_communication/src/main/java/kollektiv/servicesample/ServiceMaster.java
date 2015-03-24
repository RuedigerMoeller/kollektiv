package kollektiv.servicesample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;
import org.nustaq.kontraktor.Promise;

import java.util.*;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceMaster extends Actor<ServiceMaster> {

    HashMap<String,List<ServiceDescription>> services = new HashMap<>();

    public void $registerService( ServiceDescription desc ) {
        System.out.println("register service '"+desc.getName()+"'at "+desc.getHost()+":"+desc.getPort());
        List<ServiceDescription> serviceDescriptions = services.get(desc.getName());
        if ( serviceDescriptions == null ) {
            serviceDescriptions = new ArrayList<>();
            services.put(desc.getName(),serviceDescriptions);
        }
        serviceDescriptions.add(desc);
    }

    public void $main(String a[]) {
        List<Class<? extends Actor>> servicesToStart = new ArrayList<>(Arrays.asList( ServiceA.class, ServiceB.class, ServiceC.class ));
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
                                allNodesStarted.signal();
                            }
                            // registering is done by each service on its own.
                            //$registerService(new ServiceDescription());
                        }
                    });

                }
            });
            master.$onMemberRem(memberDesc -> {

            });
            allNodesStarted.then( () -> {
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
