# encoding:utf-8
# lijiwei@gmail.com, copyright@2017

import time, logging, struct

import pyodbc

import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import serial

class modBusSrv:
    def __init__(self, comcfg):
        self.comcfg = comcfg
        self.servers, self.slaves = [], {}

        for cfg in comcfg:
            try:
                server = modbus_rtu.RtuServer(serial.Serial(cfg[0], baudrate=9600, bytesize=8, parity='N', stopbits=1))
                for majorId, minorId in cfg[1]:
                    slave = server.add_slave(majorId)
                    slave.add_block('0', cst.ANALOG_INPUTS, 32001+(minorId-1)*50, 25)
                    slave.set_values('0', 32001+(minorId-1)*50, [257]*25)
                    sid = (majorId<<5) + minorId
                    self.slaves[sid] = slave
                self.servers.append(server)
            except Exception as e:
                logging.error(str(e), exc_info=True)
        
        for s in self.servers:
            s.start()
    
    def setValues(self, majorId, minorId, values):
        try:
            if any(v == None for v in values) or (len(values) != 3):
                raise Exception('三关键数据不全')

            vs= [0]
            oilh = struct.unpack('>2H', struct.pack('>i', int(values[0] * 1000)))
            vs.extend(oilh)
            vs.extend(oilh)
            vs.extend([0,0]) #water
            tempr = [struct.unpack('>H', struct.pack('>h', int(values[2] * 100)))[0]] * 13 
            vs.extend(tempr)
            
            pressv = struct.unpack('>5H', 
                b'\x00' + struct.pack('>i', int(values[1] * 1000)) + b'\x00\x00\x00\x01\x01')
            vs.extend(pressv)

            sid = (majorId<<5) + minorId
            if sid in self.slaves:
                self.slaves[sid].set_values('0', 32001+(minorId-1)*50, vs)
        except Exception as e:
            inf = "{} {} {} {}".format(e, majorId, minorId, values)
            logging.warn(inf, exc_info=True)


    def endSrv(self):
        for s in self.servers:
            s.stop()


def servDb2Mod(info, comcfg, interval=1.0, rows=96):
    try:
        plcMap = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25,26,
            27,28,29,30,31,32,33,34,35,36,37,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,
            54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,17,38,72,75,78,81,84,87,
            90,93,73,76,79,82,85,88,91,94,74,77,80,83,86,89,92,95]

        constr = r'DSN={};UID={};PWD={}'.format(info['dsn'], info['uid'], info['pwd'])
        conn = pyodbc.connect(constr)
        try:
            logging.info('数据库已成功连接')            
            srv = modBusSrv(comcfg)
            if not srv.servers:
                raise Exception('所有RTU串口均打开失败')

            curs = conn.cursor()
            sql = 'select top {} * from FloatTable  order by DateAndTime desc, Millitm desc'.format(rows)

            while True:
                result = curs.execute(sql)

                values = [None]*rows
                for r in result:
                    values[ plcMap[r.TagIndex] ] = r.Val

                for ma, mi in sorted(sum((c[1] for c in comcfg), [])):
                    srv.setValues(ma, mi, values[ma*3-3:ma*3])

                time.sleep(interval)
        except Exception as e:
            logging.warn(str(e), exc_info=True)
        finally:
            srv.endSrv()  #must
            conn.close()
    except Exception as e:
        logging.error(str(e), exc_info=True) #connect fail

if __name__ == '__main__':
    import os, sys, configparser
    config = configparser.ConfigParser()
    pth = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.realpath(__file__))
    config.read(os.path.join(pth, 'huaxin.ini'))

    dataSec = config['data']
    info = {n:dataSec[n] for n in ('dsn', 'uid', 'pwd')}

    try:
        interval = float(dataSec['interval'])
    except:
        interval = 1.0

    addrSec = config['addr']
    cfg = [(opt, [(int(n),1) for n in addrSec[opt].split(',')]) for opt in addrSec]
    
    while True:
        servDb2Mod(info, cfg, interval)
        time.sleep(0.5)
