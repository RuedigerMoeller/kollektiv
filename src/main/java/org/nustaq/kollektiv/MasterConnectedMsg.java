package org.nustaq.kollektiv;

import java.io.Serializable;

/**
 * Created by ruedi on 22/03/15.
 */
public class MasterConnectedMsg implements Serializable {

    private final KollektivMaster master;

    public MasterConnectedMsg(KollektivMaster master) {
        this.master = master;
    }

    public KollektivMaster getMaster() {
        return master;
    }
}
