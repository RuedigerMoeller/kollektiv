package org.nustaq.kollektiv;

import org.nustaq.kontraktor.Actor;
import org.nustaq.kontraktor.RemoteConnection;

import java.io.Serializable;

/**
 * Created by ruedi on 20/03/15.
 */
public class ActorDescription implements Serializable {

    String clazzName;

    public ActorDescription( Actor act ) {
        clazzName = act.getActor().getClass().getName();
    }

    public ActorDescription(int remoteId, String clazzName) {
        this.clazzName = clazzName;
    }

    public String getClazzName() {
        return clazzName;
    }

    @Override
    public String toString() {
        return "ActorDescription{" +
                "clazzName='" + clazzName + '\'' +
                '}';
    }
}
