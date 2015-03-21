package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by moelrue on 3/6/15.
 */
public class MemberDescription implements Serializable {

    String nodeId;
    String host;
    int numCores;

    KollektivMember member;
    String classpath;
    transient long lastHB = System.currentTimeMillis();

    List<Actor> remotedActors = new ArrayList<>();

    public MemberDescription( KollektivMember memberRef, String nodeId, int allowedCores) {
        this.nodeId = nodeId;
        this.member = memberRef;
        host = findHost();
        numCores = allowedCores;
        classpath = System.getProperty("java.class.path").replace(File.pathSeparator,":;:").replace("\\","/");
    }

    public static String findHost() {
        String hname = "?";
        try {
            hname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hname = System.getenv("COMPUTERNAME");
            if (hname == null)
                hname = System.getenv("HOSTNAME");
            if ( hname == null ) {
                try {
                    Process proc = Runtime.getRuntime().exec("hostname");
                    try (InputStream stream = proc.getInputStream()) {
                        try (Scanner s = new Scanner(stream).useDelimiter("\\A")) {
                            hname = s.hasNext() ? s.next() : "UNKNOWN";
                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return hname;
    }

    /**
     * @return fixed classpath to avoid transmission of jars which are already at members classpath
     */
    public String getClasspath() {
        return classpath;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getNumCores() {
        return numCores;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public KollektivMember getMember() {
        return member;
    }

    public void setMember(KollektivMember member) {
        this.member = member;
    }

    @Override
    public String toString() {
        return "MemberDescription{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", numCores=" + numCores +
                '}';
    }

    public long getLastHB() {
        return lastHB;
    }

    public void updateHeartbeat() {
        lastHB = System.currentTimeMillis();
    }

    /**
     * @return a list of active actors on the member.
     */
    public List<Actor> getRemotedActors() {
        return remotedActors;
    }
}
