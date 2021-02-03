package ru.ertelecom.kafka.extract.web_domru.udf;

public class IpToLong {
    String ip = "";
    public  Long ipToLong(String ip){
        String[] arr = ip.split("\\.");
        return (Long.valueOf(arr[0]) << 24)
                + (Long.valueOf(arr[1]) << 16)
                + (Long.valueOf(arr[2]) << 8)
                + Long.valueOf(arr[3]);

    }
}
