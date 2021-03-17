#  Author: J. Kaeppel

from pymodbus.client.sync import ModbusTcpClient as Client


class ModbusClient():
    def __init__(self, config):
        self.unit = int(config['MODBUS_UNIT'])
        self.address = int(config['MODBUS_ADDRESS'])
        self.count = int(config['MODBUS_COUNT'])
        self.url = config['MODBUS_URL']
        self.port = int(config['MODBUS_PORT'])

    def run(self):
        client = Client(self.url, port=self.port)
        client.connect()
        registers = []
        count = self.count
        readCount = 0
        try:
            while count > 0:
                if count > 125:
                    rr = client.read_holding_registers(self.address + readCount - 1, 125, unit=self.unit)
                    count -= 125
                    readCount += 125
                else:
                    rr = client.read_holding_registers(self.address + readCount - 1, count, unit=self.unit)
                    count = 0
                    readCount += count
                registers = registers + rr.registers
            client.close()
            return registers
        except:
            return []
