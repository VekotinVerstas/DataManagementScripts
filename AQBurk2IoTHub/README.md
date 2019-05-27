# AQBurk data to Azure IoT Hub

This script is used to query data from InfluxDB and 
send aggregated data (e.g. 5 minutes average) to Azure IoT Hub.

See documentation:
https://github.com/Azure/azure-iot-sdk-python

# Getting started

Install Azure `iothub_client` python module into you virtualenv. 
In some cases it can be installed using `pip`,
but you probably need to compile it by yourself to make it work.

Activate your python3.6+ virtualenv first.
Running `pip install -r requirements.txt` should install all necessary modules. 
After this try to run `python aqburk2iothub.py`.

You must compile the module, if you get error like:
```
ImportError: libboost_python-py35.so.1.58.0: cannot open shared object file: No such file or directory
```  

## Building for linux
```
cd /opt  # make sure you have write rights in /opt or where ever you plan to checkout the repo
git clone --recursive https://github.com/Azure/azure-iot-sdk-python.git
cd azure-iot-sdk-python/build_all/linux
sudo apt update
sudo apt install -y git cmake build-essential curl libcurl4-openssl-dev libssl-dev uuid-dev
python3 -V  # Check python version, e.g. 3.6.7
./setup.sh --python-version 3.6
./build.sh --build-python 3.6
ls -lp ../../device/samples/iothub_client.so  # Check that the binary exists
```

This command below **may** copy the binary to correct dir. If it fails, 
find `iothub_client` directory  and copy `iothub_client.so` there. 
```
cp -v ../../device/samples/iothub_client.so $(readlink -f $(dirname $(which python))/../lib/python*/site-packages/iothub_client)
```

Now `python aqburk2iothub.py` should work without ImportError.

# Usage 

```
python mqtt2iothub.py -db aqburk -m aqburk -tl 5 \
                      --devid /path/to/devids.txt --log INFO
```

# TODO

* Make querying data generic
* Use mapping table to map InfluxDB measurement fields to output data