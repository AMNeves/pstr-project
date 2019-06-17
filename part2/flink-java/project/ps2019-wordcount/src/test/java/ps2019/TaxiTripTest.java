package ps2019;

import org.junit.jupiter.api.Test;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaxiTripTest {

        @Test
        public void testTimestamp() throws Exception{
                String record = "07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,01/01/2013 00:00,01/01/2013 00:02,120,0.44,-73.956528,40.716976,-73.96244,40.715008,CSH,3.5,0.5,0.5,0,0,4.5";
                TaxiTrip trip = new TaxiTrip();
                //trip.fromString(record);
                // "dd/MM/yyyy HH:mm:ss" "yyyy-MM-dd HH:mm:ss"
                SimpleDateFormat time_formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm");
                Date date = time_formatter.parse("01/01/2013 00:02");
        }

}
