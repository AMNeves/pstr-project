package ps2019;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class DebsSchema implements DeserializationSchema<TaxiTrip>, SerializationSchema<TaxiTrip> {

        @Override
        public TaxiTrip deserialize(byte[] bytes) throws IOException {
            return TaxiTrip.fromString(new String(bytes));
        }

        @Override
        public byte[] serialize(TaxiTrip myMessage) {
            return myMessage.toString().getBytes();
        }

        @Override
        public TypeInformation<TaxiTrip> getProducedType() {
            return TypeExtractor.getForClass(TaxiTrip.class);
        }

        // Method to decide whether the element signals the end of the stream.
        // If true is returned the element won't be emitted.
        @Override
        public boolean isEndOfStream(TaxiTrip myMessage) {
            return false;

    }
}
