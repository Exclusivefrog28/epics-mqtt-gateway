package org.excf.epicsmqtt.gateway.mqtt;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.util.UUID;

public class MQTTMessage {
    private final String topic;
    private final byte[] payload;
    private UUID uuid;

    public MQTTMessage(Mqtt5Publish message) {
        this.topic = message.getTopic().toString();
        this.payload = message.getPayloadAsBytes();

        message.getUserProperties().asList().stream()
                .filter(prop -> prop.getName().toString().equals("UUID"))
                .map(prop -> prop.getValue().toString())
                .findFirst().ifPresent(uuidString -> uuid = UUID.fromString(uuidString));
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayloadAsBytes() {
        return payload;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MQTTMessage && ((MQTTMessage) obj).uuid.equals(uuid);
    }

}
