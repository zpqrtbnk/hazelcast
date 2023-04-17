package com.hazelcast.jet.dotnet;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.BootstrappedInstanceProxy;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.core.JetTestSupport.smallInstanceWithResourceUploadConfig;

public class LambdaTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceWithResourceUploadConfig();
        initialize(1, config);
    }

//    @Test
//    public void Test() {
//        HazelcastInstance hzb = Hazelcast.bootstrappedInstance();
//        BootstrappedInstanceProxy hzp = (BootstrappedInstanceProxy) hzb;
//        //     public HazelcastInstance getInstance() { return instance; }
//        HazelcastInstanceProxy hzi = (HazelcastInstanceProxy) hzp.getInstance();
//        SerializationService svc = hzi.getSerializationService();
//
//        // all this works in tests so WHAT is wrong on the other machine?
//
//        //FunctionEx<Integer, Integer> f = (i) -> i+1;
//        Object o = new Example();
//        Data data = svc.toData(o);
//        Object oo = svc.toObject(data);
//
//        // should example expose some sort of version ID?
//        // https://stackoverflow.com/questions/19421176/serialization-of-an-object-of-a-class-on-2-different-jvms-in-java
//        // but that would trigger a class exception, not a lambda exception
//        //
//        // and...
//        // the remote end *has* the hazelcast-jet-dotnet JAR so it should know all about
//        // AHAHAHAHA but not about services and all that shit, that we keep modifying!!!!
//        // should we just drop the JAR from remote and pass it along all the time, at least for now?
//    }
}
