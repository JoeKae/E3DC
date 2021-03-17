# E3DC
E3DC Inverter

<h3>set directory</h3>
<h4>run.py</h4>
Line 10: directory = '/path/to/directory/'

<h3>set inverter address</h3>
<h4>config.ini</h4>
Line 13: MODBUS_URL	= e3dc.inverter.lan

<h3>set MQTT broker address</h3>
<h4>config.ini</h4>
Line 17: BROKER	= mqtt.broker.lan

<h3>set MQTT topics</h3>
<h4>config.ini</h4>
Line 20-23:</br>
ALL = energy/pv/inverter/e3dc/0/all</br>
BATTERY = energy/pv/inverter/e3dc/0/battery</br>
GRID = energy/main/grid</br>
HOME = energy/main/home</br>



