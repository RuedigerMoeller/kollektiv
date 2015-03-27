import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.nustaq.kollektiv.ConnectionType;
import org.nustaq.kollektiv.KollektivMaster;
import org.nustaq.kollektiv.KollektivMember;
import org.nustaq.kollektiv.MemberDescription;
import org.nustaq.kontraktor.util.Log;

/**
 * Control Utility which can act as a Master Node for a Kollektiv Cluster. Note that you can connect only if
 * the Cluster's master node is not running.
 */
public class Kontrol {

    @Parameter(names={"-p","-port"}, description = "define the port serving on")
    int port = KollektivMember.DEFAULT_PORT;
    @Parameter(names={"-s","-stopActors"}, description = "send a signal to all members to stop any hosted actor")
    boolean stop = false;
    @Parameter(names={"-r","-reboot"}, description = "send a signal to all members to attempt a restart")
    boolean reboot = false;
    @Parameter(names={"-k","-kill"}, description = "send a signal to all members to terminate")
    boolean terminate = false;
    @Parameter(names={"-changeMaster", "-cm"}, description = "change the address of the master members try to connect. Careful, a wrong host:port string can make members unreachable forever ..")
    String retarget = null;
    @Parameter(names = {"-h","-help","-?", "--help"}, help = true, description = "display help")
    boolean help;
    @Parameter(names = {"-t","-time"}, description = "number of seconds to run and display add/remove messages.")
    int duration = 2;

    @Override
    public String toString() {
        return "Kontrol{" +
                   "port=" + port +
                   ", stop=" + stop +
                   ", reboot=" + reboot +
                   ", terminate=" + terminate +
                   ", retarget='" + retarget + '\'' +
                   ", help=" + help +
                   ", duration=" + duration +
                   '}';
    }

    public static void main( String a[] ) throws Exception {

        System.out.println("===================================");
        System.out.println("==       kollektiv.KONTROL       ==");
        System.out.println("===================================");
        Kontrol options = new Kontrol();
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

        System.out.println();
        System.out.println("start listening for members on "+options);

        KollektivMaster master = KollektivMaster.Start(options.port, ConnectionType.Passive, null );
        System.out.println("server started");

        master.$onMemberAdd( member -> {
            System.out.println("Member added "+member);
            if ( options.stop ) {
                member.getMember().$shutdownAllActors();
            }
            if ( options.terminate ) {
                member.getMember().$terminate(3000);
            }
            if ( options.reboot ) {
                member.getMember().$restart(3000);
            }
        });
        master.$onMemberRem((member) -> {
            System.out.println("Member removed " + member);
        });

        Thread.sleep(options.duration*1000);

        java.util.List<MemberDescription> members = master.getMembers();
        System.out.println();
        System.out.println("Summary");
        System.out.println("=======");
        System.out.println("Members("+members.size()+"):");
        members.forEach(member -> {
            System.out.println(member);
            member.getMember().$allActorNames().await().forEach( actorDescription -> System.out.println( "  "+actorDescription ));
        });

        System.exit(-1);
    }
}
