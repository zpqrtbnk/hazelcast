package com.hazelcast.jet.ext.client;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.jet.ext.client.protocol.JetDebugCodec;
import com.hazelcast.jet.ext.client.protocol.JetSubmitYamlJobCodec;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>(2);
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    public void initFactories() {
        factories.put(JetSubmitYamlJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetSubmitYamlJobMessageTask(cm, node, con));
        factories.put(JetDebugCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetDebugMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
