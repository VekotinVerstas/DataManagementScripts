# MQTT to InfluxDB bridge

[mqtt2influxdb.py](mqtt2influxdb.py) script listens to a topic in an MQTT broker, 
decodes received messages
and stores the data into InfluxDB database. 

# Gettings started

Copy `config.ini.example` --> `config.ini`. Check and fix mqtt configuration.

Install `requirements.txt`, then run:

`python mqtt2influxdb.py --help`

## Example

```
python mqtt2influxdb.py  -db ruuvigw -m ruuvitag -t 'ruuvigw/+/RuuviTag/#' \
                         -f ruuvi --config config.ini --log DEBUG
```

# Supported message formats

Mqtt2InfluxDB supports currenlty 2 message formats: custom JSON format and RuuviTag RAW format.
Add `-f {jsonsensor,ruuvi}` switch to choose which format is processed.

## Custom JSON

Custom JSON is and object, which has `mac`, `sensor` and `data` keys. 
Messages are generated e.g. in ESP8266 running Vekotinverstas' 
[DemoSensor](https://github.com/vekotinVerstas/DemoSensor) firmware.
 
```
{
   "mac" : "12:34:56:78:90:AB:CD:EF",
   "sensor" : "bme680",
   "data" : {
      "humi" : 28.3,
      "gas" : 108.9,
      "temp" : 27.64,
      "pres" : 1009.61
   }
}
```

`mac` is devices unique identifier.  
`sensor` is unique name for this sensor type (e.g. bme280, sds011 etc.)  
`data` contains one or more key-value pairs, where key is measurement's name and value is a float. 


## RuuviTag RAW

### Raw format 3

Raw:  
`1558683263:03491661C76003CEFEFBFFEF0B5F`

Decoded:  
```
{
  "data_format": 3,
  "humidity": 36.5,
  "temperature": 22.97,
  "pressure": 1010.4,
  "acceleration": 1008.5068170319921,
  "acceleration_x": 974,
  "acceleration_y": -261,
  "acceleration_z": -17,
  "battery": 2911
}
```

### Raw format 5

Raw:  
`1558683264:0512DE31F9C6BBFFD8002C040C9BB62CB628D7DCB71C528F'`

Decoded:
```
{
  "data_format": 5,
  "humidity": 31.98,
  "temperature": 24.15,
  "pressure": 1008.75,
  "acceleration": 1037.705160438166,
  "acceleration_x": -40,
  "acceleration_y": 44,
  "acceleration_z": 1036,
  "tx_power": 4,
  "battery": 2845,
  "movement_counter": 44,
  "measurement_sequence_number": 46632,
  "mac": "1234567890ab"
}
```

# TODO

Work on progress...

* optimise code