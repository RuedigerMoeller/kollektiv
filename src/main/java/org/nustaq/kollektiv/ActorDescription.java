package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.RemoteConnection;

import java.io.Serializable;

/**
 * Created by ruedi on 20/03/15.
 */
public class ActorDescription implements Serializable {

    int remoteId;
    String clazzName;

    public ActorDescription( Actor act ) {
        remoteId = ((RemoteConnection)act.__connections.peek()).getRemoteId(act);
        clazzName = act.getActor().getClass().getName();
    }

    public ActorDescription(int remoteId, String clazzName) {
        this.remoteId = remoteId;
        this.clazzName = clazzName;
    }

    public int getRemoteId() {
        return remoteId;
    }

    public String getClazzName() {
        return clazzName;
    }

    @Override
    public String toString() {
        return "ActorDescription{" +
                "remoteId=" + remoteId +
                ", clazzName='" + clazzName + '\'' +
                '}';
    }
}
