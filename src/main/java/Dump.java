import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.Actors;
import org.nustaq.kontraktor.util.Log;

/**
 * Created by ruedi on 16/03/15.
 */
public class Dump {

    @Parameter(names={"-p","-port"}, description = "define the port serving on")
    int port = 3456;
    @Parameter(names={"-s","-stopActors"}, description = "send a signal to all members to stop any hosted actor")
    boolean stop = false;
    @Parameter(names={"-r","-reboot"}, description = "send a signal to all members to attempt a restart")
    boolean reboot = false;
    @Parameter(names={"-changeMaster", "-cm"}, description = "change the address of the master members try to connect. Careful, a wrong host:port string can make members unreachable forever ..")
    String retarget = null;
    @Parameter(names = {"-h","-help","-?", "--help"}, help = true, description = "display help")
    boolean help;
    @Parameter(names = {"-t","-time"}, description = "number of seconds to run and display add/remove messages.")
    int duration = 2;

    @Override
    public String toString() {
        return "Options{" +
                "port=" + port +
                ", reboot=" + reboot +
                ", duration=" + duration +
                '}';
    }

    public static void main( String a[] ) throws Exception {
        Dump options = new Dump();
        JCommander com = new JCommander();
        com.addObject(options);
        try {
            com.parse(a);
        } catch (Exception ex) {
            System.out.println("command line error: '"+ex.getMessage()+"'");
            options.help = true;
        }
        if ( options.help ) {
            com.usage();
            System.exit(-1);
        }

        Log.Lg.$setSeverity(Log.WARN);

        System.out.println("start listening for members on "+options);

        KollektivMaster master = KollektivMaster.Start(options.port, ConnectionType.Passive );
        System.out.println("server started");

        System.out.println();
        master.$onMemberAdd( member -> {
            System.out.println("Member added "+member);
            if ( options.stop ) {
                member.getMember().$shutdownAllActors();
            }
            return false;
        });
        master.$onMemberRem((member) -> {
            System.out.println("Member removed " + member);
            return false;
        });

        Thread.sleep(options.duration*1000);

        java.util.List<MemberDescription> members = master.getMembers();
        System.out.println();
        System.out.println("Summary");
        System.out.println("=======");
        System.out.println("Members("+members.size()+"):");
        members.forEach(member -> {
            System.out.println(member);
            Actors.sync(member.getMember().$allActorNames()).forEach( actorDescription -> System.out.println( "  "+actorDescription ));
        });

        System.exit(-1);
    }
}
