package com.nokia.fni.wificloud;

import org.apache.spark.sql.types.*;

public class NwccSchema {
    public static StructType getNwccSchema() {
        // Define the schema for the StaInfoAVRO record
        StructType staInfoSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("mac_address", DataTypes.StringType, true),
            DataTypes.createStructField("ip_address", DataTypes.StringType, true),
            DataTypes.createStructField("hostname", DataTypes.StringType, true),
            DataTypes.createStructField("dl_throughput", DataTypes.LongType, true),
            DataTypes.createStructField("ul_throughput", DataTypes.LongType, true),
            DataTypes.createStructField("tx_packets", DataTypes.LongType, true),
            DataTypes.createStructField("rx_packets", DataTypes.LongType, true),
            DataTypes.createStructField("tx_bytes", DataTypes.LongType, true),
            DataTypes.createStructField("rx_bytes", DataTypes.LongType, true),
            DataTypes.createStructField("tx_bitrate", DataTypes.LongType, true),
            DataTypes.createStructField("rx_bitrate", DataTypes.LongType, true),
            DataTypes.createStructField("signal", DataTypes.StringType, true),
            DataTypes.createStructField("tx_per", DataTypes.LongType, true),
            DataTypes.createStructField("htMode", DataTypes.StringType, true),
            DataTypes.createStructField("capBits", DataTypes.LongType, true),
            DataTypes.createStructField("assoc_time", DataTypes.LongType, true)
        });

        // Define the schema for the NeighborApInfoAVRO record
        StructType neighborApInfoSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("mac_address", DataTypes.StringType, true),
                DataTypes.createStructField("ssid", DataTypes.StringType, true),
                DataTypes.createStructField("freq", DataTypes.LongType, true),
                DataTypes.createStructField("signal_strength", DataTypes.LongType, true)
        });

        // Define the schema for the ChannelInfoAVRO record
        StructType channelInfoSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("channelFreq", DataTypes.LongType, true),
                DataTypes.createStructField("noise", DataTypes.LongType, true),
                DataTypes.createStructField("channelObss", DataTypes.LongType, true),
                DataTypes.createStructField("channelDuration", DataTypes.LongType, true),
                DataTypes.createStructField("channelLoad", DataTypes.LongType, true),
                DataTypes.createStructField("channelIbss", DataTypes.LongType, true),
                DataTypes.createStructField("badPlcp", DataTypes.LongType, true),
                DataTypes.createStructField("medium_available", DataTypes.LongType, true),
                DataTypes.createStructField("channelOwnUsageRatio", DataTypes.LongType, true)
        });

        // Define the schema for the GeoLocationAVRO record
        StructType geoLocationSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("latitude", DataTypes.StringType, true),
                DataTypes.createStructField("longitude", DataTypes.StringType, true),
                DataTypes.createStructField("altitude", DataTypes.StringType, true),
                DataTypes.createStructField("posx", DataTypes.StringType, true),
                DataTypes.createStructField("posy", DataTypes.StringType, true),
                DataTypes.createStructField("posz", DataTypes.StringType, true)
        });
        
        // Define the schema for the RadioAVRO record
        StructType radioSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("frequency", DataTypes.LongType, true),
                DataTypes.createStructField("interface_name", DataTypes.StringType, true),
                DataTypes.createStructField("mac_address", DataTypes.StringType, true),
                DataTypes.createStructField("ssid", DataTypes.StringType, true),
                DataTypes.createStructField("dl_throughput", DataTypes.LongType, true),
                DataTypes.createStructField("ul_throughput", DataTypes.LongType, true),
                DataTypes.createStructField("tx_power", DataTypes.StringType, true),
                DataTypes.createStructField("num_of_stas", DataTypes.LongType, true),
                DataTypes.createStructField("tx_packets", DataTypes.LongType, true),
                DataTypes.createStructField("rx_packets", DataTypes.LongType, true),
                DataTypes.createStructField("signal", DataTypes.StringType, true),
                DataTypes.createStructField("tx_per", DataTypes.StringType, true),
                DataTypes.createStructField("rx_per", DataTypes.StringType, true),
                DataTypes.createStructField("tx_prr", DataTypes.StringType, true),
                DataTypes.createStructField("channel_own_usage_ratio", DataTypes.LongType, true),
                DataTypes.createStructField("sta_info", DataTypes.createArrayType(staInfoSchema), true),
                DataTypes.createStructField("bw", DataTypes.LongType, true),
                DataTypes.createStructField("neighbor_ap_info", DataTypes.createArrayType(neighborApInfoSchema), true)
        });

        // Define the schema for the nwccAp record
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("tenantId", DataTypes.StringType, true),
                DataTypes.createStructField("subscriberId", DataTypes.StringType, true),
                DataTypes.createStructField("mac_address", DataTypes.StringType, true),
                DataTypes.createStructField("sourceType", DataTypes.StringType, true),
                DataTypes.createStructField("zoneTag", DataTypes.StringType, true),
                DataTypes.createStructField("reportPeriod", DataTypes.LongType, false),
                DataTypes.createStructField("createdAt", DataTypes.LongType, true),
                DataTypes.createStructField("ap_manufacturer", DataTypes.StringType, true),
                DataTypes.createStructField("radio_interface", DataTypes.createArrayType(radioSchema), true),
                DataTypes.createStructField("channel_info", DataTypes.createArrayType(channelInfoSchema), true),
                DataTypes.createStructField("geo_location", geoLocationSchema, true)
        });
    }
}
