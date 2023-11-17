package com.nokia.fni.wificloud;

public class Sql {
    public static final String RAW_DATA_SQL = "/* \r\n" + //
            " This spark sql is constructed according to the nwcc periodic metric schema. if schema is changed this sql should be modified accordingly \r\n" + //
            " The spark sql datatype of each column is derived  FROM corresponding nwcc avro schema types \r\n" + //
            " */\r\n" + //
            "SELECT\r\n" + //
            "    main.tenantId,\r\n" + //
            "    main.subscriberId,\r\n" + //
            "    main.mac_address,\r\n" + //
            "    main.sourceType,\r\n" + //
            "    main.zoneTag,\r\n" + //
            "    main.reportPeriod,\r\n" + //
            "    cast((main.createdAt/1000) as long) as ts,\r\n" + //
            "    main.ap_manufacturer,\r\n" + //
            "    named_struct(\r\n" + //
            "        'frequency',\r\n" + //
            "        main.radio.frequency,\r\n" + //
            "        'frequency_band',\r\n" + //
            "        main.frequency_band,\r\n" + //
            "        'interface_name',\r\n" + //
            "        main.radio.interface_name,\r\n" + //
            "        'mac_address',\r\n" + //
            "        main.radio.mac_address,\r\n" + //
            "        'ssid',\r\n" + //
            "        main.radio.ssid,\r\n" + //
            "        'dl_throughput',\r\n" + //
            "        main.radio.dl_throughput,\r\n" + //
            "        'ul_throughput',\r\n" + //
            "        main.radio.ul_throughput,\r\n" + //
            "        'tx_power',\r\n" + //
            "        main.radio.tx_power,\r\n" + //
            "        'num_of_stas',\r\n" + //
            "        main.radio.num_of_stas,\r\n" + //
            "        'tx_packets',\r\n" + //
            "        main.radio.tx_packets,\r\n" + //
            "        'rx_packets',\r\n" + //
            "        main.radio.rx_packets,\r\n" + //
            "        'signal',\r\n" + //
            "        main.radio.signal,\r\n" + //
            "        'tx_per',\r\n" + //
            "        main.radio.tx_per,\r\n" + //
            "        'rx_per',\r\n" + //
            "        main.radio.rx_per,\r\n" + //
            "        'tx_prr',\r\n" + //
            "        main.radio.tx_prr,\r\n" + //
            "        'channel_own_usage_ratio',\r\n" + //
            "        main.radio.channel_own_usage_ratio,\r\n" + //
            "        'bw',\r\n" + //
            "        main.radio.bw\r\n" + //
            "    ) AS radio_interface,\r\n" + //
            "    main.station AS sta_info,\r\n" + //
            "    channeltable.channel AS channel_info,\r\n" + //
            "    main.geo_location\r\n" + //
            "FROM\r\n" + //
            "    (\r\n" + //
            "        SELECT\r\n" + //
            "            *,\r\n" + //
            "            radio,\r\n" + //
            "            station,\r\n" + //
            "            CASE\r\n" + //
            "                WHEN radio.frequency between 2400 and 2499 THEN '2.4GHz'\r\n" + //
            "                WHEN radio.frequency between 5000 and 5999 THEN '5GHz'\r\n" + //
            "                ELSE 'Unknown'\r\n" + //
            "            END as frequency_band\r\n" + //
            "        FROM\r\n" + //
            "            nwcc\r\n" + //
            "            LATERAL VIEW OUTER explode(radio_interface) AS radio\r\n" + //
            "            LATERAL VIEW OUTER explode(radio.sta_info) AS station\r\n" + //
            "    ) AS main\r\n" + //
            "    LEFT OUTER JOIN (\r\n" + //
            "        SELECT\r\n" + //
            "            subscriberId,\r\n" + //
            "            mac_address,\r\n" + //
            "            createdAt,\r\n" + //
            "            channel\r\n" + //
            "        FROM\r\n" + //
            "            nwcc\r\n" + //
            "            LATERAL VIEW OUTER explode(channel_info) AS channel\r\n" + //
            "    ) AS channeltable ON main.subscriberId = channeltable.subscriberId\r\n" + //
            "    AND main.mac_address = channeltable.mac_address\r\n" + //
            "    AND main.createdAt = channeltable.createdAt\r\n" + //
            "    AND main.radio.frequency = channeltable.channel.channelFreq";
    public static final String ASSOCIATED_DEVICE_SQL = "SELECT concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.interface_name,\r\n" + //
            "        \"_\",\r\n" + //
            "        sta_info.hostname,\r\n" + //
            "        \"_\",\r\n" + //
            "        sta_info.mac_address\r\n" + //
            "    ) AS monitored_point_id,\r\n" + //
            "    concat(\r\n" + //
            "        sta_info.hostname,\r\n" + //
            "        \"_\",\r\n" + //
            "        sta_info.mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.interface_name,\r\n" + //
            "        \"_\",\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address\r\n" + //
            "    ) AS monitored_point_name,\r\n" + //
            "    \"associatedDevice\" AS monitored_point_type,\r\n" + //
            "    \"streaming\" AS period_type,\r\n" + //
            "    ts,\r\n" + //
            "    concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.interface_name\r\n" + //
            "    ) AS parent_monitored_point_id,\r\n" + //
            "    subscriberId as account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.interface_name AS accesspoint_interface_name,\r\n" + //
            "    radio_interface.mac_address AS accesspoint_mac_address,\r\n" + //
            "    radio_interface.ssid AS ssid,\r\n" + //
            "    radio_interface.frequency AS accesspoint_frequency,\r\n" + //
            "    sta_info.mac_address AS associateddevice_mac_address,\r\n" + //
            "    sta_info.hostname AS associateddevice_hostname,\r\n" + //
            "    first(sta_info.dl_throughput + sta_info.ul_throughput) as wifi_throughput,\r\n" + //
            "    first(sta_info.tx_packets + sta_info.rx_packets) as wifi_packets,\r\n" + //
            "    first(sta_info.signal) as wifi_connection_signal_observation,\r\n" + //
            "    first(sta_info.tx_per) as wifi_errorrate,\r\n" + //
            "    first(sta_info.tx_packets) AS wifi_assocdevice_stats_packetssend,\r\n" + //
            "    first(sta_info.rx_packets) AS wifi_assocdevice_stats_packetsreceived,\r\n" + //
            "    first(sta_info.assoc_time) as wifi_assocdevice_assoctime,\r\n" + //
            "    first(sta_info.tx_bytes)/(1024 * 1024) as wifi_ds_data_volume_mb,\r\n" + //
            "    first(sta_info.rx_bytes)/(1024 * 1024) as wifi_us_data_volume_mb,\r\n" + //
            "    first(sta_info.tx_bitrate) as wifi_assocdevice_txbitrate_mbps,\r\n" + //
            "    first(sta_info.rx_bitrate) as wifi_assocdevice_rxbitrate_mbps\r\n" + //
            "FROM nwcc_raw\r\n" + //
            "WHERE sta_info IS NOT NULL\r\n" + //
            "GROUP BY ts,\r\n" + //
            "    account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.interface_name,\r\n" + //
            "    radio_interface.mac_address,\r\n" + //
            "    radio_interface.ssid,\r\n" + //
            "    radio_interface.frequency,\r\n" + //
            "    sta_info.mac_address,\r\n" + //
            "    sta_info.hostname\r\n" + //
            "";
    public static final String ACCESS_POINT_SQL = "SELECT concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.interface_name\r\n" + //
            "    ) AS monitored_point_id,\r\n" + //
            "    concat(\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.interface_name,\r\n" + //
            "        \"_\",\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address\r\n" + //
            "    ) AS monitored_point_name,\r\n" + //
            "    \"accessPoint\" AS monitored_point_type,\r\n" + //
            "    \"streaming\" AS period_type,\r\n" + //
            "    ts,\r\n" + //
            "    concat(ap_manufacturer, \"_\", mac_address) AS parent_monitored_point_id,\r\n" + //
            "    subscriberId as account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.interface_name AS accesspoint_interface_name,\r\n" + //
            "    radio_interface.mac_address AS accesspoint_mac_address,\r\n" + //
            "    radio_interface.ssid AS ssid,\r\n" + //
            "    radio_interface.frequency AS accesspoint_frequency,\r\n" + //
            "    first(radio_interface.tx_packets + radio_interface.rx_packets) as wifi_packets,\r\n" + //
            "    first(radio_interface.dl_throughput + radio_interface.ul_throughput) as wifi_throughput,\r\n" + //
            "    first(radio_interface.tx_packets) AS wifi_packetssend,\r\n" + //
            "    first(radio_interface.rx_packets) AS wifi_packetsreceived,\r\n" + //
            "    first(radio_interface.tx_per + radio_interface.rx_per) as wifi_errorrate,\r\n" + //
            "    first(radio_interface.tx_power) AS wifi_txpower,\r\n" + //
            "    first(radio_interface.tx_prr) as wifi_retransmissionrate,\r\n" + //
            "    first(radio_interface.signal) as wifi_signalstrength_dbm,\r\n" + //
            "    first(radio_interface.num_of_stas) as wifi_device_associations,\r\n" + //
            "    sum(sta_info.tx_bytes)/(1024 * 1024) as wifi_ds_data_volume_mb,\r\n" + //
            "    sum(sta_info.rx_bytes)/(1024 * 1024) as wifi_us_data_volume_mb,\r\n" + //
            "    avg(sta_info.tx_bitrate) as wifi_avg_assocdevice_txbitrate_mbps,\r\n" + //
            "    max(sta_info.tx_bitrate) as wifi_max_assocdevice_txbitrate_mbps,\r\n" + //
            "    min(sta_info.tx_bitrate) as wifi_min_assocdevice_txbitrate_mbps,\r\n" + //
            "    avg(sta_info.rx_bitrate) as wifi_avg_assocdevice_rxbitrate_mbps,\r\n" + //
            "    max(sta_info.rx_bitrate) as wifi_max_assocdevice_rxbitrate_mbps,\r\n" + //
            "    min(sta_info.rx_bitrate) as wifi_min_assocdevice_rxbitrate_mbps,\r\n" + //
            "    first(channel_info.noise) as wifi_noise_dbm,\r\n" + //
            "    first(radio_interface.signal / channel_info.noise) as wifi_device_snr_db\r\n" + //
            "FROM nwcc_raw\r\n" + //
            "GROUP BY ts,\r\n" + //
            "    account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.interface_name,\r\n" + //
            "    radio_interface.mac_address,\r\n" + //
            "    radio_interface.ssid,\r\n" + //
            "    radio_interface.frequency\r\n" + //
            "    \r\n" + //
            "    ";
    public static final String RADIO_INTERFACE_SQL = "SELECT\r\n" + //
            "    concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address,\r\n" + //
            "        \"_\",\r\n" + //
            "        radio_interface.frequency_band,\r\n" + //
            "        \"_Radio\"\r\n" + //
            "    ) AS monitored_point_id,\r\n" + //
            "    concat(\r\n" + //
            "        radio_interface.frequency_band,\r\n" + //
            "        \"_Radio_\",\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address\r\n" + //
            "    ) AS monitored_point_name,\r\n" + //
            "    \"radioInterface\" as monitored_point_type,\r\n" + //
            "    \"streaming\" as period_type,\r\n" + //
            "    ts,\r\n" + //
            "    concat(ap_manufacturer, \"_\", mac_address) as parent_monitored_point_id,\r\n" + //
            "    subscriberId as account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.frequency_band as frequency_band,\r\n" + //
            "    sum(radio_interface.tx_packets) as wifi_packetssend,\r\n" + //
            "    sum(radio_interface.rx_packets) as wifi_packetsreceived,\r\n" + //
            "    sum(radio_interface.tx_packets + radio_interface.rx_packets) as wifi_packets,\r\n" + //
            "    avg(radio_interface.tx_per + radio_interface.rx_per) as wifi_avg_errorrate,\r\n" + //
            "    avg(radio_interface.tx_power) as wifi_avg_txpower,\r\n" + //
            "    avg(radio_interface.tx_prr) as wifi_avg_retransmissionrate,\r\n" + //
            "    sum(radio_interface.dl_throughput + radio_interface.ul_throughput) as wifi_throughput,\r\n" + //
            "    avg(radio_interface.signal) as wifi_avg_assocdevice_signalstrength_dbm,\r\n" + //
            "    sum(radio_interface.num_of_stas) as wifi_device_associations\r\n" + //
            "FROM (\r\n" + //
            "        SELECT\r\n" + //
            "            subscriberId,\r\n" + //
            "            mac_address,\r\n" + //
            "            ts,\r\n" + //
            "            ap_manufacturer,\r\n" + //
            "            first(radio_interface) as radio_interface\r\n" + //
            "        FROM nwcc_raw\r\n" + //
            "        GROUP BY ts,\r\n" + //
            "        subscriberId,\r\n" + //
            "        mac_address,\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        radio_interface.interface_name,\r\n" + //
            "        radio_interface.mac_address,\r\n" + //
            "        radio_interface.ssid,\r\n" + //
            "        radio_interface.frequency\r\n" + //
            "    )\r\n" + //
            "GROUP BY ts,\r\n" + //
            "    account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    radio_interface.frequency_band";
    public static final String MESH_ROUTER_SQL = "SELECT concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address\r\n" + //
            "    ) AS monitored_point_id,\r\n" + //
            "    concat(\r\n" + //
            "        ap_manufacturer,\r\n" + //
            "        \"_\",\r\n" + //
            "        mac_address\r\n" + //
            "    ) AS monitored_point_name,\r\n" + //
            "    \"meshRouter\" AS monitored_point_type,\r\n" + //
            "    \"streaming\" AS period_type,\r\n" + //
            "    ts,\r\n" + //
            "    'null' AS parent_monitored_point_id,\r\n" + //
            "    subscriberId as account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer,\r\n" + //
            "    sum(radio_interface.tx_packets + radio_interface.rx_packets) as wifi_packets,\r\n" + //
            "    sum(radio_interface.dl_throughput + radio_interface.ul_throughput) as wifi_throughput,\r\n" + //
            "    sum(radio_interface.tx_packets) AS wifi_packetssend,\r\n" + //
            "    sum(radio_interface.rx_packets) AS wifi_packetsreceived,\r\n" + //
            "    avg(radio_interface.tx_per + radio_interface.rx_per) as wifi_avg_errorrate,\r\n" + //
            "    avg(radio_interface.tx_power) AS wifi_avg_txpower,\r\n" + //
            "    min(radio_interface.tx_power) AS wifi_min_txpower,\r\n" + //
            "    max(radio_interface.tx_power) AS wifi_max_txpower,\r\n" + //
            "    avg(radio_interface.tx_prr) as wifi_avg_retransmissionrate,\r\n" + //
            "    avg(radio_interface.signal) as wifi_avg_signalstrength_dbm,\r\n" + //
            "    min(radio_interface.signal) as wifi_min_signalstrength_dbm,\r\n" + //
            "    max(radio_interface.signal) as wifi_max_signalstrength_dbm,\r\n" + //
            "    sum(radio_interface.num_of_stas) as wifi_device_associations,\r\n" + //
            "    sum(sta_info.tx_bytes)/(1024 * 1024) as wifi_ds_data_volume_mb,\r\n" + //
            "    sum(sta_info.rx_bytes)/(1024 * 1024) as wifi_us_data_volume_mb,\r\n" + //
            "    avg(sta_info.tx_bitrate) as wifi_avg_assocdevice_txbitrate_mbps,\r\n" + //
            "    max(sta_info.tx_bitrate) as wifi_max_assocdevice_txbitrate_mbps,\r\n" + //
            "    min(sta_info.tx_bitrate) as wifi_min_assocdevice_txbitrate_mbps,\r\n" + //
            "    avg(sta_info.rx_bitrate) as wifi_avg_assocdevice_rxbitrate_mbps,\r\n" + //
            "    max(sta_info.rx_bitrate) as wifi_max_assocdevice_rxbitrate_mbps,\r\n" + //
            "    min(sta_info.rx_bitrate) as wifi_min_assocdevice_rxbitrate_mbps,\r\n" + //
            "    avg(radio_interface.signal / channel_info.noise) as wifi_avg_device_snr_db,\r\n" + //
            "    min(radio_interface.signal / channel_info.noise) as wifi_min_device_snr_db,\r\n" + //
            "    max(radio_interface.signal / channel_info.noise) as wifi_max_device_snr_db\r\n" + //
            "FROM nwcc_raw\r\n" + //
            "GROUP BY ts,\r\n" + //
            "    account_id,\r\n" + //
            "    mac_address,\r\n" + //
            "    ap_manufacturer";
}
