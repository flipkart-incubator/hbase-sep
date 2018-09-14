package com.flipkart.transact.yak.sep;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.flipkart.yak.sep.SerDe;
import com.flipkart.yak.sep.SerDeException;
import com.flipkart.yak.sep.proto.SepMessageProto.SepMessage;
import com.flipkart.yak.sep.proto.SepMessageProto.SepTableName;
import com.google.protobuf.ByteString;

public class SerDeTest {

    @Test
    public void test() {
        SepTableName sepTable = SepTableName.newBuilder()
                .setNamespace(ByteString.copyFrom("test".getBytes()))
                .setQualifier(ByteString.copyFrom("new_table".getBytes()))
                .build();
        SepMessage msg = SepMessage
                .newBuilder()
                .setTable(sepTable)
                .setTimestamp(System.currentTimeMillis())
                .setRow(ByteString.copyFrom(UUID.randomUUID().toString()
                        .getBytes()))
                .setValue(ByteString.copyFrom("Sample value".getBytes()))
                .setColumnfamily(ByteString.copyFrom("cf".getBytes()))
                .setQualifier(ByteString.copyFrom("data".getBytes()))
                .build();
        byte[] data = msg.toByteArray();

        try {
            SepMessage newMsg = SerDe.DESERIALZIER.execute(data);
            Assert.assertEquals(msg, newMsg);
        } catch (SerDeException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

}
