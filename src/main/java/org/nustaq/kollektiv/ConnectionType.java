package org.nustaq.kollektiv;

/**
* Created by ruedi on 21/03/15.
*/
public enum ConnectionType {
    Connect,    // stops all currently runnning actors and redefines classes
    Reconnect,  // just connect to already running actors. Assume classes are in sync
    Passive     // passive connectionType, only known classes can be used (generic tool support)
}
