import struct
import serial
import modbus_tk.modbus_rtu as tkRtu
import logging
import time

#协议处理接口类：metaclass, wyprotocal, master, getSlaveData
class MetaRegCls(type):
    protoClsReg = {}
    def __new__(cls, name, bases, attrs):
        newcls = super().__new__(cls, name, bases, attrs)
        pro = getattr(newcls, 'wyprotocal', None)
        if pro:
            MetaRegCls.protoClsReg[pro] = newcls
        return newcls

    @staticmethod
    def getClass(proto):
        return MetaRegCls.protoClsReg.get(proto, None)

# rtu-modbus level gause
class rtuModLGChannel(metaclass=MetaRegCls):
    def __init__(self, portName, rtutimeout):
        try:
            se = serial.Serial(port=portName, baudrate=self.baudrate, parity=self.parity, timeout=rtutimeout)
            logging.disable(logging.ERROR) #temporarily
            self.master = tkRtu.RtuMaster(se)
            logging.disable(logging.NOTSET)
            self.master.set_timeout(rtutimeout)
        except Exception as e:
            self.master = None
            logging.warning('%s Open failed, %s'%(portName, e))

    def getSlaveData(self, slaveId, minorId):
        return None

    def portName(self):
        return self.master._serial.name if self.master else ""

    def close(self):
        if self.master:
            self.master.close()

class mtsModbusCh(rtuModLGChannel):
    wyprotocal = 'mts_modbus'
    baudrate, parity = 4800, 'N'

    def getSlaveData(self, slaveId, minorId):
        try:
            r = self.master.execute(slaveId, 4, 0, 18, data_format=">9i")
            #r = (1000000, 51000,0, 276000, 266000, 246000, 226000, 216000, 181000)

            rd = dict(oilh = r[0]/1000., waterh = r[1]/1000., avg_temp = r[8]/10000., _type=1) 
            rd.update( ('temp%d'%i, r[i+2]/10000) for i in range(1,6) )
            return rd
        except Exception as e:
            pass
        
        return None


class htFmuCh(rtuModLGChannel, metaclass=MetaRegCls):
    wyprotocal = 'ht_fmu'
    baudrate, parity = 9600, 'N'

    def getSlaveData(self, slaveId, minorId):
        try:
            '''油高 水高 温度 温度[5] 压力 原始油高 储罐状态'''
            r = self.master.execute(slaveId, 4, 32001+(minorId-1)*50, 25, data_format='>2B3i13hBibi')
            r, reserved = r[:-1], r[-1]
            #r = (3, 0, 8194000, 8194000, 210000, 1829, 2760, 2660, 2460, 2260, 2160, -1, -1, -1, -1, -1, -1, -1, 0, 79000, 0)
            if r[1] > 0: #数据状态出错
                raise Exception

            rd = dict(oilh=r[3]/1000., waterh=r[4]/1000., avg_temp=r[5]/100.,
                        pressvalue = r[-2]/1000. if r[-3]<15 else 0. , #ret[-3]高4位非0 则压力数据错/丢失
                        orig_oilh = r[2]/1000., rstatus = r[-3], _type=2, _simu=reserved) 
            rd.update( ('temp%d'%i, r[i+5]/100.) for i in range(1,6) )
            return rd
        except Exception as e:
            pass
        
        return None


class htTpuCh(rtuModLGChannel, metaclass=MetaRegCls):
    wyprotocal = 'ht_tpu'
    baudrate, parity = 9600, 'N'

    def getSlaveData(self, slaveId, minorId):
        try:
            '''油高 水高 温度 温度[5] 压力 原始油高 储罐状态'''
            '''视密 计重密度 标密 VCF 净油体积 标准体积 水体积 质量 空容量'''
            r = self.master.execute(slaveId, 4, 32001+(minorId-1)*50, 25, data_format='>2B3I13hBIb4x')
            #r = (5, 0, 6390000, 6390000, 60000, 1850, 2760, 2660, 2460, 2260, 2160, -1, -1, -1, -1, -1, -1, -1, 0, 79000, 0)
            if r[1] > 0: #数据状态
                raise Exception
            time.sleep(0.1)

            r2 = self.master.execute(slaveId, 4, 33001+(minorId-1)*30, 15, data_format='>b3h3b5I')
            #r2 = (5, 7420, 7300, 7480, 0, 39, 0, 15234, 14290, 1523, 256, 4562325)

            rd = dict(oilh=r[3]/1000., waterh=r[4]/1000., avg_temp=r[5]/100.,
                        pressvalue = r[-2]/1000. if r[-3]<15 else 0. , #ret[-3]高4位非0 则压力数据错/丢失
                        orig_oilh = r[2]/1000., rstatus = r[-3], _type=3) 
            rd.update( ('temp%d'%i, r[i+5]/100.) for i in range(1,6) )

            rd.update(zip(['sden', 'jzden', 'cdensity'], (r2[i]/10 for i in range(1,4))))
            rd.update(vcf=int.from_bytes(r2[4:7],'big')/100000.)
            rd.update(zip(['oilvol', 'v20', 'watervol', 'mass', 'emptyvol'], (r2[i] for i in range(7,12))))

            return rd
        except Exception as e:
            pass
        
        return None


class mtsDDAChannel(metaclass=MetaRegCls):
    wyprotocal = 'mts_dda'
    baudrate, parity = 4800, 'E'

    def __init__(self, portName, timeout):
        try:
            self.master = serial.Serial(port=portName, baudrate=self.baudrate, parity=self.parity, timeout=timeout)
            self.caches = {} 
        except Exception as e:
            self.master = None
            logging.warning('%s Open failed, %s'%(portName, e))

    def command(self, slaveId, cmd):
        try:
            xcmd = bytearray((slaveId,cmd))
            self.master.write(xcmd)

            buf, exp = bytearray(), 6
            while exp:
                ret = self.master.read(exp)
                if len(ret) != exp:
                    raise Exception('DDA未获足够数据')

                buf.extend(ret)
                if exp < 6:
                    break
                try:
                    exp = ret.index(3)
                except Exception as e:
                    exp = 6

            start = buf.index(2)
            assert (start>=2 and buf[start-1] == cmd and buf[start-2] == slaveId and len(buf)>9), "DDA数据错误"
            assert (sum(buf[start:-5]) + int(buf[-5:]) == 65536), "DDA检验失败"

            return buf[start+1:-6]
        except Exception as e:
            logging.debug(str(e))

        return None

    def cachedCmd(self, slaveId, cmd, timeout):
        cache = self.caches.setdefault(cmd, {})
        # cache is a dict of slaveID with (data, time) elements
        cd = cache.get(slaveId, None)
        if cd and (time.time()-cd[1]) < timeout:
            return cd[0]

        #missed or expired
        ret = None
        _timeout = self.master.timeout
        for i in range(2):
            time.sleep(0.1)
            ret = self.command(slaveId, cmd)
            if ret:
                cache[slaveId] = ret, time.time()
                break
            else:
                self.master.timeout = max(self.master.timeout * 2, 4)
        self.master.timeout = _timeout

        return ret

    def getSlaveData(self, slaveId, minorId):
        try:
            #_timeout = self.master.timeout
            dinfo = "[{}, {}]".format(self.portName(), slaveId)

            units = 48, 48    #℉(0)/℃(1) ,英寸(0)/毫米(4)。 【错误理解】
            #ret = self.command(slaveId, 80) # b'\x50' 单位
            ret = self.cachedCmd(slaveId, 80, 180.)
            if ret:
                units = ret[2], ret[4]
            else:
                raise Exception("DDA读取单位失败: " + dinfo)

            time.sleep(0.1)
            ret = self.command(slaveId, 18) #\0x12 液位
            
            if ret:
                data = ret.decode('ascii').strip().split(':')
                levels = [0.0] * len(data)
                for i, s in enumerate(data):
                    try:
                        levels[i] = float(s) * (1. if units[1]==52 else 25.4)
                    except ValueError:
                        logging.info(dinfo+' 液位数据存在错误 '+s)
                #levels = [float(s)*(1. if units[1]==52 else 25.4) for s in data]

                d = dict(oilh=levels[0], waterh=levels[1], _type=1)
                d.update( ('temp%d'%i, -200.) for i in range(6) )

                #time.sleep(0.1)
                #ret = self.command(slaveId, 33) #温度 \0x21 0.02, \0x20, 0.2
                ret = self.cachedCmd(slaveId, 33, 40.)
                if ret:
                    data = ret.decode('ascii')
                    errored = False
                    for i, s in enumerate(data.split(':')):
                        try:
                            fv = float(s)
                            if units[0] != 49:
                                fv = (fv-32)/1.8
                            d['temp%d'%i] = fv
                        except:
                            d['temp%d'%i] = -100.
                            errored = True
                    if errored:
                        logging.warning('DDA温度点数据错: ' + data)
                else:
                    logging.warning("DDA读取温度失败: " + dinfo)
                    #raise Exception("DDA读取温度失败")
                d['avg_temp'] = d.pop('temp0')

                return d
            else:
                raise Exception("DDA读取液位失败: " + dinfo)
        except Exception as e:
            #self.master.timeout = _timeout
            logging.warning(str(e))

        return None

    def portName(self):
        return self.master.name if self.master else ""

    def close(self):
        if self.master:
            self.master.close()


# 主机查询命令
#FF FF FF FF FF 82 A6 06 BC 61 4E 01 00 B0
# 从机响应
#FF FF FF FF FF 86 A6 06 BC 61 4E 01 07 00 00 06 40 B0 00 00 45
class comDPTChannel(metaclass=MetaRegCls):
    wyprotocal = 'hart'
    baudrate, parity = 1200, 'O'

    def __init__(self, portName, timeout):
        try:
            self.master = serial.Serial(port=portName, baudrate=self.baudrate, parity=self.parity, timeout=timeout)
        except Exception as e:
            self.master = None
            logging.warning('%s Open failed, %s'%(portName, str(e)))

    def getSlaveData(self, mfgId, devType, devId):
        #return 7.5
        try:
            data = bytearray(b'\xff'*5 + b'\x82')
            data.append(mfgId)
            data.append(devType)
            data.extend(devId.to_bytes(3, 'big'))
            data.append(1) #b'\x01'
            data.append(0) #b'\x00'

            chk = 0
            for i in data[5:]:
                chk = chk^i

            data.append(chk)
            #data.append(64) # b'\x40'，可选

            self.master.write(data)
            ret = self.master.read(21)
            if len(ret)==21:
                chk = 0
                for i in data[5:]:
                    chk = chk^i
                assert (ret[:5] == data[:5] and ret[5]==134 and ret[6:12]==data[6:12] 
                        and ret[12]==7 and chk==0), 'HART校验失败'
                p = struct.unpack('>f', ret[16:20])[0] #big
                return p
            #print('PRESS (%s, %d, %d, %d) failed'%(self.portName(),mfgId, devType, devId))
        except Exception as e:
            logging.warning(str(e))

        return None

    def portName(self):
        return self.master.name if self.master else ""

    def close(self):
        if self.master:
            self.master.close()


class sndAlmChannel(metaclass=MetaRegCls):
    wyprotocal = 'brHTACA1.0'
    baudrate, parity = 9600, 'N'

    def __init__(self, portName, rtutimeout):
        try:
            se = serial.Serial(port=portName, baudrate=self.baudrate, parity=self.parity, timeout=rtutimeout)
            logging.disable(logging.ERROR) #temporarily
            self.master = tkRtu.RtuMaster(se)
            logging.disable(logging.NOTSET)
            self.master.set_timeout(rtutimeout)
        except Exception as e:
            self.master = None
            logging.warning('%s Open failed, %s'%(portName, e))

    def allSet(self, on, num=4):
        try:
            data = [1 if on else 0] * num
            r = self.master.execute(0xfe, 0x0f, 0, output_value=data)
        except Exception as e:
            logging.info("全开全闭指令失败:" + str(e))

    def portName(self):
        return self.master._serial.name if self.master else ""

    def close(self):
        if self.master:
            self.master.close()

if __name__ == '__main__':
    import time
    #from serial.tools.list_ports import comports
    #for port, desc, hwid in sorted(comports()):
    #    print("%s: %s [%s]" % (port, desc, hwid))

    assert MetaRegCls.getClass('mts_modbus') == mtsModbusCh

    timeout = 1.0
    C2 = mtsModbusCh('COM2', timeout)
    C4 = htFmuCh('COM4',  timeout)
    C6 = htTpuCh('COM6', timeout)
    C8 = comDPTChannel('COM8',timeout)
    C10 = mtsDDAChannel('COM10', timeout)

    if True:
        time.sleep(2.0)

        print('\n', C2.wyprotocal)
        for m,n in [(193,0),(194,0),(197,0)]:
            tx1 = time.time()
            print(C2.getSlaveData(m, n), time.time()-tx1)

        print('\n', C4.wyprotocal)
        for m,n in [(195,3)]:
            tx1 = time.time()
            print(C4.getSlaveData(m, n), time.time()-tx1)

        print('\n', C6.wyprotocal)
        for m,n in [(197,5),(198,6)]:
            print(C6.getSlaveData(m, n))

        print('\n', C8.wyprotocal)
        for m,t,d in [(166,6,3325604), (166,6,3325605), (166,6,3325603)]:
            print(C8.getSlaveData(m,t,d))

        print('\n', C10.wyprotocal)
        for m,n in [(199, 0)]:
            print(C10.getSlaveData(m, n))
