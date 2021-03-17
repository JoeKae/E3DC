#  Author: P. Stoy / J. Kaeppel

from scheduler import scheduler
from loguru import logger
from e3dc import inverter
import configparser
import sys
import datetime

directory = '/path/to/directory/'

logger.add(directory+'logfile.log', rotation="500 KB")
state = 100

try:
    logger.info('Service starting')
    if state == 100:
        try:
            logger.info('Config loading started')
            config = configparser.ConfigParser()
            config.read(directory+'config.ini')
            state = 200
            logger.info('Config loading finished - State {}', state)
        except:
            state = 199
            logger.warning('Config loading error - ' + str(sys.exc_info()[1]))
    if state == 200:
        try:
            logger.info('Creating E3DC instance')
            e3dc = inverter.E3dc(config['E3DC_S10_0'])
            state = 300
            logger.info('E3DC instance created - State {}', state)
        except:
            state = 299
            logger.warning('E3DC instance could not be created')
    
    if state == 300:
        try:
            logger.info('Creating MqttClient instance')
            e3dc.setMqttClient(config.get('MQTT', 'BROKER'))
            e3dc.mqttEnablePublishAll(config.get('MQTT_TOPIC', 'ALL'))
            e3dc.mqttEnablePublishBattery(config.get('MQTT_TOPIC', 'BATTERY'))
            e3dc.mqttEnablePublishGrid(config.get('MQTT_TOPIC', 'GRID'))
            e3dc.mqttEnablePublishHome(config.get('MQTT_TOPIC', 'HOME'))
            state = 400
            logger.info('MqttClient instance created - State {}', state)
        except:
            state = 399
            logger.warning('MqttClient instance could not be created')

    if state == 400:
        try:
            logger.info('Scheduler initialization started')
            taskScheduler = scheduler.Scheduler().schedulerGenerator()
            job = taskScheduler.add_job(e3dc.refresh, 'interval', seconds=int(config.get('SCHEDULER', 'INTERVAL')), next_run_time=datetime.datetime.now())
            state = 500
            logger.info('Scheduler initialization finished - State {}', state)
        except:
            state = 499
            logger.warning('Scheduler initialization error - ' + str(sys.exc_info()[1]))
    if state == 500:
        try:
            logger.info('Service started successful')
            logger.info('Service is running')
            taskScheduler.start()
        except:
            pass
except:
    pass
