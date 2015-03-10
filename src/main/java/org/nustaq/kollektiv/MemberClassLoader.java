package org.nustaq.kollektiv;

import org.nustaq.kontraktor.impl.ClassPathProvider;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ruedi on 07/03/15.
 */
public class MemberClassLoader extends URLClassLoader implements ClassPathProvider {

    ActorAppBundle bundle;
    List<File> jars = new ArrayList<>();
    private File base;

    public MemberClassLoader(ActorAppBundle bun, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        bundle = bun;
    }

    public void addURL(File jar) {
        try {
            super.addURL(jar.toURL());
            jars.add(jar);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public List<File> getJars() {
        return jars;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loadedClass = findLoadedClass(name);
            if ( loadedClass != null ) {
                return loadedClass;
            }
//            if ( //name.startsWith(ActorProxyFactory.class.getName()) ||
//                name.startsWith( TestActor.class.getName() ) // debug
//               )
//            {
//                Class<?> res = findClass(name);
//                if ( res != null ) {
//                    return res;
//                }
//            }
            Class<?> res;
            try {
                res = super.loadClass(name, resolve);
            } catch (ClassNotFoundException cnfe ) {
                res = findClass(name);
                if ( res == null )
                    throw cnfe;
                resolveClass(res);
            }
            return res;
        }
    }

    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return super.findClass(name);
//        byte[] aClass = bundle.findClass(name);
//        if ( aClass != null ) {
//            System.out.println("** load class ** "+name);
//            return defineClass(aClass, 0, aClass.length);
//        }
//        else
//            return null;
    }

    public void setBase(File base) {
        this.base = base;
    }

    public File getBase() {
        return base;
    }

    @Override
    public List<File> getClassPath() {
        ArrayList<File> res = new ArrayList<>(jars);
        res.add(base);
        return res;
    }
}
