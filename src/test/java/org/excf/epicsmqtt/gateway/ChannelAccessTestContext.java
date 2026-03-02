package org.excf.epicsmqtt.gateway;

import com.cosylab.epics.caj.CAJContext;
import gov.aps.jca.configuration.ConfigurationException;
import gov.aps.jca.configuration.DefaultConfiguration;

public class ChannelAccessTestContext extends CAJContext {
    public static ChannelAccessTestContext get() throws ConfigurationException {
        ChannelAccessTestContext context = new ChannelAccessTestContext();
        DefaultConfiguration config = new DefaultConfiguration("CONTEXT");
        context.addressList = "127.0.0.1:5074";
        context.autoAddressList = false;

        context.configure(config);

        return context;
    }
}
