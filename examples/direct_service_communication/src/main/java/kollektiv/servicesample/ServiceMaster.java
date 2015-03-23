package kollektiv.servicesample;

import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.Future;

import java.util.*;

import static org.nustaq.kontraktor.Actors.*;

/**
 * Created by ruedi on 23/03/15.
 */
public class ServiceMaster extends Actor<ServiceMaster> {

    HashMap<String,List<ServiceDescription>> services = new HashMap<>();

    public void $registerService( ServiceDescription desc ) {
        List<ServiceDescription> serviceDescriptions = services.get(desc.getName());
        if ( serviceDescriptions == null ) {
            serviceDescriptions = new ArrayList<>();
            services.put(desc.getName(),serviceDescriptions);
        }
        serviceDescriptions.add(desc);
    }

    public void $main(String a[]) {
        List<Class<? extends Actor>> servicesToStart = Arrays.asList( ServiceA.class, ServiceB.class, ServiceC.class );
        try {
            KollektivMaster master = KollektivMaster.Start(KollektivMember.DEFAULT_PORT, ConnectionType.Connect, self());
            master.$onMemberAdd( memberDesc -> {
                if ( servicesToStart.size() > 0 ) {
                    Class<? extends Actor> toStart = servicesToStart.remove(servicesToStart.size() - 1);
                    master.$run(memberDesc.getMember(), toStart).then( (remoteRef,error) -> {
                        if ( error != null ) {
                            servicesToStart.add(toStart);
                            System.out.println("Failed to start actor");
                        } else {
                            remoteRef.$receive( new InitMsg(self()) );
                            // registering is done by each service on its own.
                            //$registerService(new ServiceDescription());
                        }
                    });
                }
                return false;
            });
            master.$onMemberRem(memberDesc -> {
                return false;
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
