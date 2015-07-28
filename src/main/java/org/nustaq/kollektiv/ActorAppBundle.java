package org.nustaq.kollektiv;
import java.io.Serializable;
import java.util.*;

/**
 * Created by moelrue on 3/6/15.
 */
public class ActorAppBundle implements Serializable {

    public static class CPEntry implements Serializable {
        byte bytes[];
        String name;

        public CPEntry(byte[] bytes, String name) {
            this.bytes = bytes;
            this.name = name;
        }
    }

    HashMap<String,CPEntry> resources = new HashMap<>();

    transient String baseDir;

    public void put(String normalizedPath, byte[] bytes) {
        resources.put(normalizedPath, new CPEntry(bytes, normalizedPath));
    }

    public void setResources(HashMap<String, CPEntry> resources) {
        this.resources = resources;
    }

    public HashMap<String, CPEntry> getResources() {
        return resources;
    }

    public int getSizeKB() {
        int sum = 0;
        for (Iterator<Map.Entry<String, CPEntry>> iterator = resources.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, CPEntry> next = iterator.next();
            sum += next.getValue().bytes.length;
        }
        return sum/1000;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

}
