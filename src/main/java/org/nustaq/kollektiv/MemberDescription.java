package org.nustaq.kollektiv;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

    public MemberDescription( KollektivMember memberRef, String nodeId, int allowedCores) {
        this.nodeId = nodeId;
        this.member = memberRef;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = System.getenv("COMPUTERNAME");
            if (host == null)
                host = System.getenv("HOSTNAME");
            if ( host == null ) {
                try {
                    Process proc = Runtime.getRuntime().exec("hostname");
                    try (InputStream stream = proc.getInputStream()) {
                        try (Scanner s = new Scanner(stream).useDelimiter("\\A")) {
                            host = s.hasNext() ? s.next() : "UNKNOWN";
                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        numCores = allowedCores > 0 ? allowedCores : Runtime.getRuntime().availableProcessors();
        classpath = System.getProperty("java.class.path").replace(File.pathSeparator,":;:").replace("\\","/");
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

}
