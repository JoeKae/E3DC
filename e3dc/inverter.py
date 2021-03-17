#  Author: J. Kaeppel

from ctypes import *
from datetime import datetime
from modbus import modbus
import paho.mqtt.client as mqtt
import json


class Ems:
    bat_charge_disable = None
    bat_discharge_disable = None
    emergency_power_availaible = None
    weather_based_charge_blocking = None
    max_grid_feed_cap = None
    bat_charge_disable_by_schedule = None
    bat_discharge_disable_by_schedule = None

    def update(self, value):
        self.bat_charge_disable = ((value & (1 << 0)) > 0)
        self.bat_discharge_disable = ((value & (1 << 1)) > 0)
        self.emergency_power_availaible = ((value & (1 << 2)) > 0)
        self.weather_based_charge_blocking = ((value & (1 << 3)) > 0)
        self.max_grid_feed_cap = ((value & (1 << 4)) > 0)
        self.bat_charge_disable_by_schedule = ((value & (1 << 5)) > 0)
        self.bat_discharge_disable_by_schedule = ((value & (1 << 6)) > 0)

    def toJson(self):
        return {
            "battery charge disabled": self.bat_charge_disable,
            "battery discharge disabled": self.bat_discharge_disable,
            "emergency power feasible": self.emergency_power_availaible,
            "weather based charging": self.weather_based_charge_blocking,
            "energy production capped": self.max_grid_feed_cap,
            "charge blocked by schedule": self.bat_charge_disable_by_schedule,
            "discharge blocked by schedule": self.bat_discharge_disable_by_schedule
        }


class Wb:
    wb_available = None
    pv_only = None  # rw
    charge_disable = None  # rw
    ev_charging = None
    type_2_locked = None
    type_2_plugged_in = None
    schuko_enabled = None  # rw
    schuko_plugged_in = None
    schuko_locked = None
    relay_16a_1p_schuko = None
    relay_16a_3p_type_2 = None
    relay_32a_3p_type_2 = None
    single_phase = None  # rw

    def toJson(self):
        return {
            "available": self.wb_available,
            "pvOnly": self.pv_only,
            "chargingDisabled": self.charge_disable,
            "evIsCharging": self.ev_charging,
            "type2PlugLocked": self.type_2_locked,
            "type2PluggedIn": self.type_2_plugged_in,
            "schukoEnabled": self.schuko_enabled,
            "schukoPluggedIn": self.schuko_plugged_in,
            "schukoLocked": self.schuko_locked,
            "schukoRelay16A1P": self.relay_16a_1p_schuko,
            "type2Relay16A3P": self.relay_16a_3p_type_2,
            "type2Relay32A3P": self.relay_32a_3p_type_2,
            "phasesLimitedTo1": self.single_phase
        }

    def update(self, value):
        self.wb_available = ((value & (1 << 0)) > 0)
        self.pv_only = ((value & (1 << 1)) > 0)
        self.charge_disable = ((value & (1 << 2)) > 0)
        self.ev_charging = ((value & (1 << 3)) > 0)
        self.schuko_enabled = ((value & (1 << 4)) > 0)
        self.type_2_locked = ((value & (1 << 5)) > 0)
        self.type_2_plugged_in = ((value & (1 << 6)) > 0)
        self.schuko_plugged_in = ((value & (1 << 7)) > 0)
        self.schuko_locked = ((value & (1 << 8)) > 0)
        self.relay_16a_1p_schuko = ((value & (1 << 9)) > 0)
        self.relay_16a_3p_type_2 = ((value & (1 << 10)) > 0)
        self.relay_32a_3p_type_2 = ((value & (1 << 11)) > 0)
        self.single_phase = ((value & (1 << 12)) > 0)


class E3dc:
    mqttClient = 0
    mqttList = []

    magic_byte = None  # uni16 hex
    modbus_firmware = None  # uint8.unit8
    supported_reg_count = None  # uint16
    manufacturer = None  # String 16xuint16 print(''.join((chr(i>>8)+chr(i&0xFF)) for i in rr.registers))
    model = None  # String 16xuint16 print(''.join((chr(i>>8)+chr(i&0xFF)) for i in rr.registers))
    serial = None  # String 16xuint16 print(''.join((chr(i>>8)+chr(i&0xFF)) for i in rr.registers))
    firmware = None  # String 16xuint16 print(''.join((chr(i>>8)+chr(i&0xFF)) for i in rr.registers))

    pv_power = None  # Int32
    bat_power = None  # Int32
    drain_power = None  # Int32
    grid_power = None  # int32
    ext_power = None  # int32
    wb_power = None  # int32
    wb_pv_power = None  # int32
    self_sufficiency = None  # uint8 percent
    own_consumption = None  # uint8 percent
    bat_soc = None  # uint16 percent
    emergency_power = None  # uint16
    ems_status = None  # uint16
    ems_remote_ctrl = None  # int16
    ems_ctrl = None  # uint16
    wbs_ctrl = [None] * 8  # uint16 r/w
    dc_string_1_voltage = None  # uint16
    dc_string_2_voltage = None  # uint16
    dc_string_3_voltage = None  # uint16
    dc_string_1_current = None  # uint16
    dc_string_2_current = None  # uint16
    dc_string_3_current = None  # uint16
    dc_string_1_power = None  # uint16
    dc_string_2_power = None  # uint16
    dc_string_3_power = None  # uint16
    ac_powermeter = None
    ac_powermeter_0 = None
    ac_powermeter_0_phase_1_power = None
    ac_powermeter_0_phase_2_power = None
    ac_powermeter_0_phase_3_power = None
    ac_powermeter_1 = None
    ac_powermeter_1_phase_1_power = None
    ac_powermeter_1_phase_2_power = None
    ac_powermeter_1_phase_3_power = None
    ac_powermeter_2 = None
    ac_powermeter_2_phase_1_power = None
    ac_powermeter_2_phase_2_power = None
    ac_powermeter_2_phase_3_power = None
    ac_powermeter_3 = None
    ac_powermeter_3_phase_1_power = None
    ac_powermeter_3_phase_2_power = None
    ac_powermeter_3_phase_3_power = None
    ac_powermeter_4 = None
    ac_powermeter_4_phase_1_power = None
    ac_powermeter_4_phase_2_power = None
    ac_powermeter_4_phase_3_power = None
    ac_powermeter_5 = None
    ac_powermeter_5_phase_1_power = None
    ac_powermeter_5_phase_2_power = None
    ac_powermeter_5_phase_3_power = None
    ac_powermeter_6 = None
    ac_powermeter_6_phase_1_power = None
    ac_powermeter_6_phase_2_power = None
    ac_powermeter_6_phase_3_power = None
    modbusClient = None
    wbs = [Wb()] * 8
    ems = Ems()

    def __init__(self, config):
        self.modbusClient = modbus.ModbusClient(config)

    def setMqttClient(self, broker):
        self.mqttClient = mqtt.Client()
        self.mqttClient.connect(broker)
        self.mqttClient.loop_start()

    def mqttPublish(self):
        for pub in self.mqttList:
            if pub['id'] == 'grid':
                self.mqttClient.publish(pub['topic'], payload=self.grid_power)
            if pub['id'] == 'home':
                self.mqttClient.publish(pub['topic'], payload=self.drain_power)
            if pub['id'] == 'battery':
                data = {
                    "soc": self.bat_soc,
                    "power": self.bat_power
                }
                self.mqttClient.publish(pub['topic'], payload=json.dumps(data))
            if pub['id'] == 'all':
                allPayload = {
                    "inverter": {
                        "magicByte": self.magic_byte,
                        "modbusFirmware": self.modbus_firmware,
                        "supportedRegisterCount": self.supported_reg_count,
                        "manufacturer": self.manufacturer,
                        "model": self.model,
                        "serial": self.serial,
                        "firmwareVersion": self.firmware
                    },
                    "pv": {
                        "pvPower": self.pv_power,
                        "extPower": self.ext_power,
                        "totalPower": self.pv_power - self.ext_power,
                        "dcString1Voltage": self.dc_string_1_voltage,
                        "dcString2Voltage": self.dc_string_2_voltage,
                        "dcString3Voltage": self.dc_string_3_voltage,
                        "dcString1Current": self.dc_string_1_current,
                        "dcString2Current": self.dc_string_2_current,
                        "dcString3Current": self.dc_string_3_current,
                        "dcString1Power": self.dc_string_1_power,
                        "dcString2Power": self.dc_string_2_power,
                        "dcString3Power": self.dc_string_3_power
                    },
                    "battery": {
                        "chargeDisabled": self.ems.bat_charge_disable,
                        "dischargeDisabled": self.ems.bat_discharge_disable,
                        "weatherBasedCharging": self.ems.weather_based_charge_blocking,
                        "chargingBlockedBySchedule": self.ems.bat_charge_disable_by_schedule,
                        "dischargingBlockedBySchedule": self.ems.bat_discharge_disable_by_schedule,
                        "power": self.bat_power,
                        "soc": self.bat_soc
                    },
                    "wallbox": {
                        "power": self.wb_power,
                        "pvPower": self.wb_pv_power,
                        "wallboxes": [wb.toJson() for wb in self.wbs]
                    },
                    "ems": {
                        "status": self.ems_status,
                        "remoteControl": self.ems_remote_ctrl,
                        "control": self.ems_ctrl,
                        "energyProductionCapped": self.ems.max_grid_feed_cap,
                        "emergencyPower": {
                            "active": self.emergency_power == 1,
                            "available": self.ems.emergency_power_availaible,
                            "status": self.get_emergency_power()
                        }
                    },
                    "energy": {
                        "houseConsumption": self.drain_power,
                        "gridPower": self.grid_power,
                        "selfSufficiency": self.self_sufficiency,
                        "ownConsumption": self.own_consumption
                    },
                    "acPowermeters": self.ac_powermeter,
                    "mqttSubList": self.mqttList
                }
                self.mqttClient.publish(pub['topic'], payload=json.dumps(allPayload))

    def mqttEnablePublish(self, topic, id):
        new_pub = {
            'id': id,
            'topic': topic
        }
        for pub in self.mqttList:
            if 'id' in pub:
                if pub['id'] == id:
                    self.mqttList.remove(pub)
        self.mqttList.append(new_pub)

    def mqttDisablePublish(self, topic, id):
        pub = {
            'id': id,
            'topic': topic
        }
        self.mqttList.remove(pub)

    def mqttEnablePublishAll(self, topic):
        self.mqttEnablePublish(topic, 'all')

    def mqttEnablePublishBattery(self, topic):
        self.mqttEnablePublish(topic, 'battery')

    def mqttEnablePublishHome(self, topic):
        self.mqttEnablePublish(topic, 'home')

    def mqttEnablePublishGrid(self, topic):
        self.mqttEnablePublish(topic, 'grid')

    def refresh(self):
        self.poll()

    def poll(self):
        self.update(self.modbusClient.run())

    def toJson(self):
        wallboxes = {}
        for wb in self.wbs:
            wallboxes["wallbox_{}".format(self.wbs.index(wb))] = wb.toJson()

        production = {
            "pv": self.pv_power,
            "external": self.ext_power * -1
        }
        battery = {
            "power": self.bat_power,
            "soc": self.bat_soc
        }
        local = {
            "wallboxes": wallboxes,
            "wallbox pv power": self.wb_pv_power,
            "home": self.drain_power
        }

        return {
            "time": datetime.now().strftime("%d.%m.%y %H:%M:%S"),
            "manufacturer": self.manufacturer,
            "model": self.model,
            "modbus firmware": self.modbus_firmware,
            "emergency power": self.get_emergency_power(json=True),
            "ems": self.ems.toJson(),
            "production": production,
            "battery": battery,
            "local consumption": local,
            "grid": self.grid_power
        }

    def get_emergency_power(self, json=False):
        values = {
            0: 'Notstrom wird nicht von Ihrem Gerät unterstützt (bei Geräten der älteren Gerätegeneration, z. B. S10-SP40, S10-P5002).',
            1: 'Notstrom aktiv (Ausfall des Stromnetzes)',
            2: 'Notstrom nicht aktiv',
            3: 'Notstrom nicht verfügbar',
            4: 'Der Motorschalter des S10 E befindet sich nicht in der richtigen Position, sondern wurde manuell abgeschaltet oder nicht eingeschaltet. Hinweis: \
                 Falls der Motorschalter nicht bewusst ausgeschaltet wurde, haben Sie eventuell übersehen, den Schieberegler am Motorschalter in \
                 die Position „ON“ zu bringen (s. die folgende Abbildung zur Erläuterung).'
        }

        if json:
            return {
                "emergency power not supported": (self.emergency_power == 0),
                "emergency power active": (self.emergency_power == 1),
                "emergency power inactive": (self.emergency_power == 2),
                "emergency power not available": (self.emergency_power == 3),
                "motor switch on manual": (self.emergency_power == 4)
            }

        return values[self.emergency_power]

    def unpack_string(self, data, index, length):
        temp = data[index:][:length]
        return ''.join((chr(i >> 8) + chr(i & 0xFF)) for i in temp).rstrip('\x00')

    def to_int32(self, data, index):
        temp = data[index:][:2]
        return ((c_int32)((temp[0]) | (temp[1] << 16))).value

    def to_int16(self, data, index):
        temp = data[index:][:1]
        return ((c_int16)(temp[0])).value

    def update(self, values):
        if len(values) is 0:
            return
        self.magic_byte = values[0]
        self.modbus_firmware = float((values[1] >> 8)) + float(values[1] & 0xFF)
        self.supported_reg_count = values[2]
        self.manufacturer = self.unpack_string(values, 3, 16)
        self.model = self.unpack_string(values, 19, 16)
        self.serial = self.unpack_string(values, 35, 16)
        self.firmware = self.unpack_string(values, 51, 16)
        self.pv_power = self.to_int32(values, 67)
        self.bat_power = self.to_int32(values, 69)
        self.drain_power = self.to_int32(values, 71)
        self.grid_power = self.to_int32(values, 73)
        self.ext_power = self.to_int32(values, 75)
        self.wb_power = self.to_int32(values, 77)
        self.wb_pv_power = self.to_int32(values, 79)
        self.self_sufficiency = values[81] >> 8
        self.own_consumption = values[81] & 0xFF
        self.bat_soc = values[82]
        self.emergency_power = values[83]
        self.ems_status = values[84]
        self.ems_remote_ctrl = self.to_int16(values, 85)
        self.ems_ctrl = values[86]
        self.wbs_ctrl[0] = values[87]
        self.wbs_ctrl[1] = values[88]
        self.wbs_ctrl[2] = values[89]
        self.wbs_ctrl[3] = values[90]
        self.wbs_ctrl[4] = values[91]
        self.wbs_ctrl[5] = values[92]
        self.wbs_ctrl[6] = values[93]
        self.wbs_ctrl[7] = values[94]
        self.dc_string_1_voltage = values[95]
        self.dc_string_2_voltage = values[96]
        self.dc_string_3_voltage = values[97]
        self.dc_string_1_current = values[98]
        self.dc_string_2_current = values[99]
        self.dc_string_3_current = values[100]
        self.dc_string_1_power = values[101]
        self.dc_string_2_power = values[102]
        self.dc_string_3_power = values[103]
        self.ac_powermeter_0 = values[104]
        self.ac_powermeter_0_phase_1_power = self.to_int16(values, 105)
        self.ac_powermeter_0_phase_2_power = self.to_int16(values, 106)
        self.ac_powermeter_0_phase_3_power = self.to_int16(values, 107)
        self.ac_powermeter_1 = values[108]
        self.ac_powermeter_1_phase_1_power = self.to_int16(values, 109)
        self.ac_powermeter_1_phase_2_power = self.to_int16(values, 110)
        self.ac_powermeter_1_phase_3_power = self.to_int16(values, 111)
        self.ac_powermeter_2 = values[112]
        self.ac_powermeter_2_phase_1_power = self.to_int16(values, 113)
        self.ac_powermeter_2_phase_2_power = self.to_int16(values, 114)
        self.ac_powermeter_2_phase_3_power = self.to_int16(values, 115)
        self.ac_powermeter_3 = values[116]
        self.ac_powermeter_3_phase_1_power = self.to_int16(values, 117)
        self.ac_powermeter_3_phase_2_power = self.to_int16(values, 118)
        self.ac_powermeter_3_phase_3_power = self.to_int16(values, 119)
        self.ac_powermeter_4 = values[120]
        self.ac_powermeter_4_phase_1_power = self.to_int16(values, 121)
        self.ac_powermeter_4_phase_2_power = self.to_int16(values, 122)
        self.ac_powermeter_4_phase_3_power = self.to_int16(values, 123)
        self.ac_powermeter_5 = values[124]
        self.ac_powermeter_5_phase_1_power = self.to_int16(values, 125)
        self.ac_powermeter_5_phase_2_power = values[126]
        self.ac_powermeter_5_phase_3_power = values[127]
        self.ac_powermeter_6 = values[128]
        self.ac_powermeter_6_phase_1_power = values[129]
        self.ac_powermeter_6_phase_2_power = values[130]
        self.ac_powermeter_6_phase_3_power = values[131]

        for i in range(0, 8):
            self.wbs[i].update(self.wbs_ctrl[i])
        self.ems.update(self.ems_status)

        self.addAcPowermeter()

        if self.mqttClient != 0 and len(self.mqttList) > 0:
            self.mqttPublish()

    def addAcPowermeter(self):
        self.ac_powermeter = []
        if self.ac_powermeter_0 == 1:
            self.ac_powermeter.append({
                "powermeterId": 0,
                "unit": "Watt",
                "phase1": self.ac_powermeter_0_phase_1_power,
                "phase2": self.ac_powermeter_0_phase_2_power,
                "phase3": self.ac_powermeter_0_phase_3_power,
            })
        if self.ac_powermeter_1 == 1:
            self.ac_powermeter.append({
                "powermeterId": 1,
                "unit": "Watt",
                "phase1": self.ac_powermeter_1_phase_1_power,
                "phase2": self.ac_powermeter_1_phase_2_power,
                "phase3": self.ac_powermeter_1_phase_3_power,
            })
        if self.ac_powermeter_2 == 1:
            self.ac_powermeter.append({
                "powermeterId": 2,
                "unit": "Watt",
                "phase1": self.ac_powermeter_2_phase_1_power,
                "phase2": self.ac_powermeter_2_phase_2_power,
                "phase3": self.ac_powermeter_2_phase_3_power,
            })
        if self.ac_powermeter_3 == 1:
            self.ac_powermeter.append({
                "powermeterId": 3,
                "unit": "Watt",
                "phase1": self.ac_powermeter_3_phase_1_power,
                "phase2": self.ac_powermeter_3_phase_2_power,
                "phase3": self.ac_powermeter_3_phase_3_power,
            })
        if self.ac_powermeter_4 == 1:
            self.ac_powermeter.append({
                "powermeterId": 4,
                "unit": "Watt",
                "phase1": self.ac_powermeter_4_phase_1_power,
                "phase2": self.ac_powermeter_4_phase_2_power,
                "phase3": self.ac_powermeter_4_phase_3_power,
            })
        if self.ac_powermeter_5 == 1:
            self.ac_powermeter.append({
                "powermeterId": 5,
                "unit": "Watt",
                "phase1": self.ac_powermeter_5_phase_1_power,
                "phase2": self.ac_powermeter_5_phase_2_power,
                "phase3": self.ac_powermeter_5_phase_3_power,
            })
        if self.ac_powermeter_6 == 1:
            self.ac_powermeter.append({
                "powermeterId": 6,
                "unit": "Watt",
                "phase1": self.ac_powermeter_6_phase_1_power,
                "phase2": self.ac_powermeter_6_phase_2_power,
                "phase3": self.ac_powermeter_6_phase_3_power,
            })

    def __repr__(self):
        return '                      \n\
                Hauptanlage     : {}Watt\n\
                Nebenanlage     : {}Watt\n\
                Batterie        : {}Watt\n\
                Grid            : {}Watt\n\
                Haus            : {}Watt\n\
                Batterie SOC    : {}%\n\
                Autarkie        : {}%\n\
                Eigenverbrauch  : {}%\n\
                Notstrom bereit : {}'.format(self.pv_power, self.ext_power * -1, self.bat_power, self.grid_power,
                                             self.drain_power,
                                             self.bat_soc, self.self_sufficiency, self.own_consumption,
                                             self.ems.emergency_power_availaible)
