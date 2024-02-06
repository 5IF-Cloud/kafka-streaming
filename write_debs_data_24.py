"""
Write the debs data to a Kafka topic
"""

# model                                  object
# failure                                 int64
# vault_id                                int64
# s1_read_error_rate                    float64
# s2_throughput_performance             float64
# s3_spin_up_time                       float64
# s4_start_stop_count                   float64
# s5_reallocated_sector_count           float64
# s7_seek_error_rate                    float64
# s8_seek_time_performance              float64
# s9_power_on_hours                     float64
# s10_spin_retry_count                  float64
# s12_power_cycle_count                 float64
# s173_wear_leveling_count              float64
# s174_unexpected_power_loss_count      float64
# s183_sata_downshift_count             float64
# s187_reported_uncorrectable_errors    float64
# s188_command_timeout                  float64
# s189_high_fly_writes                  float64
# s190_airflow_temperature_cel          float64
# s191_g_sense_error_rate               float64
# s192_power_off_retract_count          float64
# s193_load_unload_cycle_count          float64
# s194_temperature_celsius              float64
# s195_hardware_ecc_recovered           float64
# s196_reallocated_event_count          float64
# s197_current_pending_sector           float64
# s198_offline_uncorrectable            float64
# s199_udma_crc_error_count             float64
# s200_multi_zone_error_rate            float64
# s220_disk_shift                       float64
# s222_loaded_hours                     float64
# s223_load_retry_count                 float64
# s226_load_in_time                     float64
# s240_head_flying_hours                float64
# s241_total_lbas_written               float64
# s242_total_lbas_read                  float64

from kafka import KafkaProducer
import pandas as pd
from time import sleep
import json

# Read the data
df = pd.read_csv(f"./resources/drivedata-small.csv")

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:29092')

# Write the data to the Kafka topic

for index, row in df.iterrows():
    date = row['date']
    # Convert the date to a timestamp long
    date = pd.to_datetime(date)
    sent_data = {
        "date": int(date.timestamp()),
        "serial_number": row['serial_number'],
        "model": row['model'],
        "failure": row['failure'],
        "vault_id": row['vault_id'],
        "smart": {
            "s1_read_error_rate": row['s1_read_error_rate'],
            "s2_throughput_performance": row['s2_throughput_performance'],
            "s3_spin_up_time": row['s3_spin_up_time'],
            "s4_start_stop_count": row['s4_start_stop_count'],
            "s5_reallocated_sector_count": row['s5_reallocated_sector_count'],
            "s7_seek_error_rate": row['s7_seek_error_rate'],
            "s8_seek_time_performance": row['s8_seek_time_performance'],
            "s9_power_on_hours": row['s9_power_on_hours'],
            "s10_spin_retry_count": row['s10_spin_retry_count'],
            "s12_power_cycle_count": row['s12_power_cycle_count'],
            "s173_wear_leveling_count": row['s173_wear_leveling_count'],
            "s174_unexpected_power_loss_count": row['s174_unexpected_power_loss_count'],
            "s183_sata_downshift_count": row['s183_sata_downshift_count'],
            "s187_reported_uncorrectable_errors": row['s187_reported_uncorrectable_errors'],
            "s188_command_timeout": row['s188_command_timeout'],
            "s189_high_fly_writes": row['s189_high_fly_writes'],
            "s190_airflow_temperature_cel": row['s190_airflow_temperature_cel'],
            "s191_g_sense_error_rate": row['s191_g_sense_error_rate'],
            "s192_power_off_retract_count": row['s192_power_off_retract_count'],
            "s193_load_unload_cycle_count": row['s193_load_unload_cycle_count'],
            "s194_temperature_celsius": row['s194_temperature_celsius'],
            "s195_hardware_ecc_recovered": row['s195_hardware_ecc_recovered'],
            "s196_reallocated_event_count": row['s196_reallocated_event_count'],
            "s197_current_pending_sector": row['s197_current_pending_sector'],
            "s198_offline_uncorrectable": row['s198_offline_uncorrectable'],
            "s199_udma_crc_error_count": row['s199_udma_crc_error_count'],
            "s200_multi_zone_error_rate": row['s200_multi_zone_error_rate'],
            "s220_disk_shift": row['s220_disk_shift'],
            "s222_loaded_hours": row['s222_loaded_hours'],
            "s223_load_retry_count": row['s223_load_retry_count'],
            "s226_load_in_time": row['s226_load_in_time'],
            "s240_head_flying_hours": row['s240_head_flying_hours'],
            "s241_total_lbas_written": row['s241_total_lbas_written'],
            "s242_total_lbas_read": row['s242_total_lbas_read']
        }
        
    }
    producer.send('debs-topic', value=json.dumps(sent_data).encode('utf-8'))
    # sleep(0.4)