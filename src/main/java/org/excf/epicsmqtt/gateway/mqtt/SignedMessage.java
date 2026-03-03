package org.excf.epicsmqtt.gateway.mqtt;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.nio.ByteBuffer;
import java.util.UUID;

public class SignedMessage {
    private final static int UUID_BYTE_LENGTH = 16;

    private String topic;
    private byte[] payload;
    private UUID uuid;

    public SignedMessage(Mqtt5Publish message) {
        this.topic = message.getTopic().toString();
        byte[] receivedPayload = message.getPayloadAsBytes();
        if (receivedPayload.length < UUID_BYTE_LENGTH)
            throw new IllegalArgumentException("Payload is too small to contain a valid UUID signature.");

        ByteBuffer buffer = ByteBuffer.wrap(receivedPayload);

        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        this.uuid = new UUID(msb, lsb);

        this.payload = new byte[buffer.remaining()];
        buffer.get(this.payload);
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayloadAsBytes() {
        return payload;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SignedMessage && ((SignedMessage) obj).uuid.equals(uuid);
    }

    public static byte[] signMessage(byte[] payload) {
        if (payload == null)
            payload = new byte[0];

        UUID messageId = UUID.randomUUID();

        ByteBuffer buffer = ByteBuffer.allocate(UUID_BYTE_LENGTH + payload.length);

        buffer.putLong(messageId.getMostSignificantBits());
        buffer.putLong(messageId.getLeastSignificantBits());

        buffer.put(payload);

        return buffer.array();
    }

}
