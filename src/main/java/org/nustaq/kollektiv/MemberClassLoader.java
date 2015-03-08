package org.nustaq.kollektiv;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by ruedi on 07/03/15.
 */
public class MemberClassLoader extends URLClassLoader {

    ActorAppBundle bundle;

    public MemberClassLoader(ActorAppBundle bun, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        bundle = bun;
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
        byte[] aClass = bundle.findClass(name);
        if ( aClass != null ) {
            System.out.println("** load class ** "+name);
            return defineClass(aClass, 0, aClass.length);
        }
        else
            return null;
    }

}
