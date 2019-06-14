package ps2019;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class TaxiTrip {

        public String medallion;
        public String hack_license;
        public Date pickup_datetime;
        public Date dropoff_datetime;
        public int trip_time_in_secs;
        public double trip_distance;
        public double pickup_longitude;
        public double pickup_latitude;
        public double dropoff_longitude;
        public double dropoff_latitude;
        public String payment_type;
        public double fare_amount;
        public double surcharge;
        public double tip_amount;
        public double tolls_amount;
        public double total_amount;

    public TaxiTrip(){}

        public static TaxiTrip fromString( String s ){
            String[] tokens = s.split( "," );
            if(tokens.length != 15) throw new RuntimeException( "Invalid record: " + s );
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
            int i = 0;
            try{
                TaxiTrip trip = new TaxiTrip();
                trip.medallion = tokens[i];
                trip.hack_license = tokens[++i];
                trip.pickup_datetime = format.parse(tokens[++i]);
                trip.dropoff_datetime = format.parse(tokens[++i]);
                trip.trip_time_in_secs = Integer.parseInt(tokens[++i]);
                trip.trip_distance = Double.parseDouble(tokens[++i]);

                trip.pickup_longitude = Double.parseDouble(tokens[++i]);
                trip.pickup_latitude = Double.parseDouble(tokens[++i]);
                trip.dropoff_longitude = Double.parseDouble(tokens[++i]);
                trip.dropoff_latitude = Double.parseDouble(tokens[++i]);

                trip.payment_type = tokens[++i];
                trip.fare_amount = Double.parseDouble(tokens[++i]);
                trip.surcharge = Double.parseDouble(tokens[++i]);
                trip.tip_amount = Double.parseDouble(tokens[++i]);
                trip.tolls_amount = Double.parseDouble(tokens[++i]);
                trip.total_amount = Double.parseDouble(tokens[++i]);

                return trip;
            }catch(Exception e){
                throw new RuntimeException("Invalid record: " + s);
            }
        }

        public String toString(){
            return String.format("%s,%s,%s,%s,%d,%f,%f,%f,%f,%f,%s,%f,%f,%f,%f,%f",
                    medallion,
                    hack_license,
                    pickup_datetime.toString(),
                    dropoff_datetime.toString(),
                    trip_time_in_secs,
                    trip_distance,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude,
                    payment_type,
                    fare_amount,
                    surcharge,
                    tip_amount,
                    tolls_amount,
                    total_amount);
    }
}
