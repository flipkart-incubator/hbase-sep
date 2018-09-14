package com.flipkart.yak.sep;

import com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;

/**
 * Abstraction for client to deserialized byte[]
 * 
 * @author gokulvanan.v
 *
 */
public enum SerDe {
    DESERIALZIER;

    public SepMessage execute(byte[] data) throws SerDeException {
        try {
            return SepMessage.parseFrom(data);
        } catch (Exception e) {
            throw new SerDeException(e);
        }
    }

}
