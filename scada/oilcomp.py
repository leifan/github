import struct, os, sys
import logging

if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
elif __file__:
    application_path = os.path.dirname(os.path.abspath(__file__))

#专用于brite类型的文件查表
def _searchBfile1(d, t, fnS, fx):
    try:
        base = int(d-fnS)//20 * 20 + fnS
        col = int((d-base)/2) + 1 # 在12个数值列中的位置, [1,10]
        fn = os.path.join(application_path, 
                        'MDB', "%s%04d%04d.dat"%(fx, base, base+20))
        with open(fn, 'rb') as f:
            f.seek(48, 0)
            startT, = struct.unpack("f", f.read(4))
            #f.seek(-48, 2)
            #endT, = struct.unpack("f", f.read(4))
            cCount, tStep = os.path.getsize(fn)//48-1, 0.25
            row = int((t - startT)/tStep + 0.5)
            if row >= 0 and row < cCount:
                f.seek((row+1)*48, 0)
                values = struct.unpack("12f", f.read(48))
                ret = values[col] + (values[col+1]-values[col])*(d- ((col-1)*2+base))/2

                return ret
    except Exception as e:
        pass
        #logging.debug(str(e), exc_info=True)

    return None

#专用于brite类型的文件查表
def _searchBfile(d, t, fnS, fx, hidden={}):
    try:
        base = int(d-fnS)//20 * 20 + fnS
        key = (fx, base)

        cache = hidden.get(key, None)
        if not cache:
            try:
                fn = os.path.join(application_path, 
                                'MDB', "%s%04d%04d.dat"%(fx, base, base+20))
                values = []     
                with open(fn, 'rb') as f:
                    f.seek(48, 0)
                    cCount, tStep = os.path.getsize(fn)//48-1, 0.25
                    for i in range(cCount):
                        vs = struct.unpack("12f", f.read(48))
                        values.extend(vs)
                hidden[key] = values, cCount, tStep
            except Exception as e:
                logging.debug(str(e), exc_info=False)
                hidden[key] = [], 0, 1

        values, cCount, tStep = hidden[key]
        if values:
            col = int((d-base)/2) + 1 # 在12个数值列中的位置, [1,10]
            startT = values[0]
            row = int((t - startT)/tStep + 0.5)
            if row >= 0 and row < cCount:
                skip = 12*row
                ret = values[col + skip] + (values[col+1 + skip]-values[col + skip])*(d- ((col-1)*2+base))/2
                return ret        
    except Exception as e:
        logging.debug(str(e), exc_info=True)

    return None


#通过标准密度平均温度查体积修正系数
def findVCF(d20, t):
    ret = _searchBfile(d20, t, 650, 'vcf')
    if ret is None or ret < 0.5:
        ret = 1.0
    return ret

#通过视密视温查标密
def findD20(den, t):
    ret = _searchBfile(den, t, 653, 'DEN')
    return 0.0 if ret is None else ret

#通过 温度、膨胀系数 计算VCF
def getVCFByAlpha(T, ALPHA20):
    '''V20 = V*[1 + (20 - T)*Alpha]  => VCF = V20/V = [1 + (20 - T)*Alpha]'''
    return 1 + (20 - T) * ALPHA20

#通过 视密度、温度、膨胀系数 计算标准密度
def getDenByAlpha(den, T, ALPHA20):
    ''' vcf = V20/V = D/D20 '''
    return den/(1 + (20 - T) * ALPHA20)

def getD20FormJZDen(jzDen,Temp,kqfl):
    ''' 通过计重密度反查标密 JzDen = (d20 - KQFL)* getVCF(d20, Temp)'''
    for i in range(-400, 400):
        tD20 = jzDen + i*0.1                        #临时标密
        tJZDen = (tD20 - kqfl)* findVCF(tD20,Temp)  #临时计重
        if abs(tJZDen - jzDen) <= 0.1:
            return tD20

    return 0.0

def getSDenFromD20(D20, Temp):
    '''通过标密D20、视温LTemp获取相应的视密度''' 
    for i in range(-400, 400):
        tLDen = D20 + i*0.1
        tDen = findD20(tLDen, Temp)
        if abs(tDen - D20) <= 0.1:
            return tLDen

    return 0.0

# 插值后的罐容表
def volTbl(height, tbl):
    i = int(height)
    try:
        #assert(tbl[i][0] == i)
        if height >= tbl[-1][0]: return tbl[-1][1]
        if height < tbl[0][0]: return tbl[0][1]
        return tbl[i][1] + (tbl[i+1][1] - tbl[i][1])/(tbl[i+1][0] - tbl[i][0])*(height-tbl[i][0])
    except Exception as e:
        #pass #未配置罐容表
        logging.error('高度{:.4f}超出罐容表范围, {}'.format(height, e))

    return 0.0

def nlCalibrate(level, nlData):
    '''非线性校准 (校准值，测量值)'''
    if nlData:
        if len(nlData) == 1:
            return level + (nlData[0][0]-nlData[0][1])
        
        if level < nlData[0][1] or level >= nlData[-1][1]:
            i = 0 if level <= nlData[0][1] else -1
            return level + (nlData[i][0]-nlData[i][1])
        
        for i in range(len(nlData)-1):
            if level < nlData[i+1][1]:
                return nlData[i][0] + (nlData[i+1][0] - nlData[i][0])*(level - nlData[i][1])/(nlData[i+1][1] - nlData[i][1])
    return level

def denCalibrate(level, denData):
    ''' 计算密度补偿量 '''
    #TODO
    return 0.0

def levelCorrect(ret, tankcfg):
    ''' 液位修正 '''
    #数据偏移
    ret['oilh'] += tankcfg['_lg']['oilHgtOffset']
    ret['waterh'] += tankcfg['_lg']['waterHgtOffset']
    if tankcfg['_lg']['tempOffset']: #温度偏移不为0
        for k in ['avg_temp'] + ['temp%d'%i for i in range(1,6)]:
            ret[k] += tankcfg['_lg']['tempOffset']
    ret['orig_oilh'] = ret['oilh']

    ret['nolinercomp'] = nlCalibrate(ret['oilh'], tankcfg['_nolinH']) - ret['oilh']
    #print('非线', ret['nolinercomp'])
    ret['dencomp'] = denCalibrate(ret['oilh'], tankcfg['_nolinH'])
    
    fTotal = ret['oilh'] + ret['nolinercomp'] + ret['dencomp'] #补偿

    if fTotal > tankcfg['height']:
        fTotal = tankcfg['height']
    if fTotal < tankcfg['_lg']['oilInvalidHgt']: #总高无效区
        fTotal = tankcfg['_lg']['oilHgtRef']

    ret['oilh'] = fTotal

    if ret['waterh'] < tankcfg['_lg']['waterInvalidHgt']: #水高无效区
        ret['waterh'] = tankcfg['_lg']['waterHgtRef']


def calDen(record, tankcfg, params):
    ''' 混合法计算密度 ，返回(标密，视密，计重) '''
    refD20 = tankcfg['density']
    kqfl = params['kqfl']

    if record['pressvalue'] > 0:
        vLocPress = tankcfg['_ps']['location'] #安装基准高度
        if tankcfg['_nolinP']:
            tpl = nlCalibrate(record['oilh'], tankcfg['_nolinP']) # 获得差压变送器的虚拟位置
            if tpl < record['oilh']:
                vLocPress = tpl

        if ((record['oilh'] > tankcfg['_ps']['location'] + tankcfg['_ps']['invalidHgt']) 
                and record['oilh'] > vLocPress):
            calcDen = (record['pressvalue'] * 1000000)/(params['gravacce']*(record['oilh']-vLocPress))

            #分两种算法，计算密度作为不同密度 (1：计重密度处理 2：视密度处理)
            Flag = 2
            if Flag == 1:
                #作为计重密度
                fD20_temp = getD20FormJZDen(calcDen,record['avg_temp'],kqfl)

                if abs(fD20_temp - refD20) < 50.:
                    if fD20_temp < 700 and fD20_temp > 870:
                        logging.debug('密度超限')
                    if 890 > fD20_temp >690:
                        record['cdensity'] = fD20_temp
                        record['vcf'] = findVCF(fD20_temp, record['avg_temp'])
                        record['sden'] = getSDenFromD20(fD20_temp,record['avg_temp'])
                        record['jzden'] = calcDen
                        return True
                logging.debug('{} 临时标密({})超差或油位低于无效位置'.format(tankcfg['number'], fD20_temp))
            else:
                # 作为油品视密度
                fD20_temp = findD20(calcDen, record['avg_temp'])

                if abs(fD20_temp - refD20) < 450.:
                    if fD20_temp < 700 and fD20_temp > 870:
                        logging.debug('密度超限')
                    if 890 > fD20_temp > 690:
                        record['cdensity'] = fD20_temp
                        record['vcf'] = findVCF(fD20_temp, record['avg_temp'])
                        record['sden'] = calcDen
                        record['jzden'] = (fD20_temp - kqfl) * record['vcf']
                        return True
                logging.debug('{} 临时标密({})超差或油位低于无效位置'.format(tankcfg['number'], fD20_temp))
    else:
        logging.debug('{} 未测到压力值，使用规定标密'.format(tankcfg['number']))

    #密度使用默认输入密度
    record['cdensity'] = refD20
    record['vcf'] = findVCF(refD20, record['avg_temp'])
    record['sden'] = getSDenFromD20(refD20, record['avg_temp'])
    record['jzden'] = (refD20 - kqfl) * record['vcf']
    return False


def calVol(ret, tankcfg, params):
    if not tankcfg['_vols']:
        return None
    #体积/质量       
    ret['watervol'] = volTbl(ret['waterh'], tankcfg['_vols']) + tankcfg['deadVol']
    allvol = volTbl(ret['oilh'], tankcfg['_vols']) + tankcfg['deadVol']
    oilvol = allvol - ret['watervol']

    fMassPan = 0.0
    if tankcfg['fltPanWeight'] > 0 and tankcfg['fltPanTop']>tankcfg['fltPanBtm']:
        if tankcfg['fltPanBtm'] < ret['oilh'] < tankcfg['fltPanTop']:  #不完全起浮
            fMassPan = tankcfg['fltPanWeight'] * (ret['oilh']-tankcfg['fltPanBtm']) /(tankcfg['fltPanTop']-tankcfg['fltPanBtm'])
        elif  ret['oilh'] > tankcfg['fltPanTop']:  #完全起浮
            fMassPan = tankcfg['fltPanWeight']
    
    ret['mass'] = oilvol/1000. * ret['jzden'] - fMassPan
    ret['oilvol'] = ret['mass'] / ret['jzden'] * 1000.
    ret['v20'] = ret['oilvol'] * ret['vcf']
    ret['emptyvol'] = tankcfg['safeVol'] - allvol
    #ret['mass'] = ret['v20']*(ret['cdensity'] -params['kqfl'])

# 注意 tankcfg尚未加入ret工况
def statusCode(ret, tankcfg, params):
    ''' 获取储罐状态 '''
    TC, DELTA = params['trendcounter'], params['trendhgtdist']
    if TC < 1:
        TC = 1

    direction = 0 
    if tankcfg['_his']:
        delta = ret['oilh'] - tankcfg['_his'][-1]['oilh']
        if delta > DELTA:
            direction = 1
        elif delta < -DELTA:
            direction = 2
        else:
            direction = 0
        #ret['_direct'] = direction  #给测漏所用

        if tankcfg['_lastdir'] == direction:
            tankcfg['_seqlen'] += 1
        else:
            tankcfg['_lastdir'] = direction
            tankcfg['_seqlen']  = 1

        lastCode = tankcfg['_his'][-1]['rstatus']
        if tankcfg['_seqlen'] >= TC:
            ret['rstatus'] = direction #改变状态

            if direction != lastCode: #处理进油报告
                exactP = tankcfg['_his'][-TC]  #使用真实起点数据
                if lastCode != 0: #结束进油报告
                    #print('结束进油报告')
                    tankcfg['_oilinRec'].append(exactP)
                if direction != 0:
                    #print('开始进油报告')
                    tankcfg['_oilinRec'].append(exactP)
        else:
            ret['rstatus'] = lastCode #延续上次状态
    else:
        tankcfg['_lastdir'] = 0
        tankcfg['_seqlen']  = 1
        ret['rstatus'] = 0
        #ret['_direct'] = 0

    return True

def alarm_product(ret, tankcfg):
    ''' 液位报警 '''
    pAlarm = ''
    lastRec = tankcfg['_his'][-1] if tankcfg['_his'] else None
    if ret['oilh'] >= tankcfg['oilUpUp']:
        pAlarm = '液位高高报'
    elif ret['oilh'] >= tankcfg['oilUp']:
        if (ret['oilh'] >= tankcfg['oilUpUp'] - 20) and lastRec and '液位高高报' in lastRec['alarm']:
            pAlarm = '液位高高报'
        else:
            pAlarm = '液位高报'
    elif ret['oilh'] <= tankcfg['oilDnDn']:
        pAlarm = '液位低低报'
    elif ret['oilh'] <= tankcfg['oilDn']:
        if (ret['oilh'] <= tankcfg['oilDnDn'] + 20) and lastRec and '液位低低报' in lastRec['alarm']:
            pAlarm = '液位低低报'
        else:
            pAlarm = '液位低报'
    
    if pAlarm:
        ret['alarm'].append(pAlarm)

def alarm_interface(ret, tankcfg):
    ''' 界位报警 '''
    alarm = ''
    lastRec = tankcfg['_his'][-1] if tankcfg['_his'] else None
    if ret['waterh'] >= tankcfg['waterUpUp']:
        alarm = '水位高高报'
    elif ret['waterh'] >= tankcfg['waterUp']:
        if (ret['waterh'] >= tankcfg['waterUpUp'] - 20) and lastRec and '水位高高报' in lastRec['alarm']:
            alarm = '水位高高报'
        else:
            alarm = '水位高报'

    if alarm:
        ret['alarm'].append(alarm)


def alarm_temperature(ret, tankcfg):
    ''' 温度报警 '''
    if ret['avg_temp'] < -80.0: # -100 / -200
        tankcfg['_tmpACount'] = tankcfg.get('_tmpACount', 0) + 1 #温度异常
        if tankcfg['_tmpACount'] >= 3:
            ret['alarm'].append('温度无效')
    else:
        tankcfg['_tmpACount'] = 0

        if ret['avg_temp'] >= tankcfg['tempUp']:
            ret['alarm'].append('温度高报')
        elif ret['avg_temp'] <= tankcfg['tempDn']:
            ret['alarm'].append('温度低报')


def handle_flowrate(ret, tankcfg, params):
    ''' 流速计算+报警 '''
    try:
        if ret['rstatus'] == 0:
            ret['flowrate'] = 0.0
        else:
            i = -params['trendcounter']
            if params['trendcounter'] > len(tankcfg['_his']):
                i = 0
            volDelta = ret['v20'] - tankcfg['_his'][i]['v20']
            timespan = (ret['dtime'] - tankcfg['_his'][i]['dtime']).total_seconds()
            ret['flowrate'] = volDelta/(timespan/60.)  #升/分
    except Exception as e:
        ret['flowrate'] = 0
        logging.warning('%s 流速计算出现错误'%tankcfg['number'], exc_info=True)
    
    frAlarm = ''
    afr = abs(ret['flowrate'])
    if afr >= tankcfg['flowUp'] >0:
        frAlarm = '流速高报'
    elif tankcfg['flowDn'] >= afr >0:
        frAlarm = '流速低报'

    if frAlarm:
        tankcfg['_fraCount'] = tankcfg.get('_fraCount', 0) + 1 #流速有报警       
    else:
        tankcfg['_fraCount'] = 0
    
    if tankcfg['_fraCount'] > tankcfg['leakXL'] >0:
        ret['alarm'].append('泄露报警')
    if tankcfg['_fraCount'] > 5:    #TODO
        ret['alarm'].append(frAlarm)

def alarm_comm():
    ''' 通信报警 '''
    #TODO

def subLeakSample(ret, tankcfg, params):
    mag = '_subLksLeft'
    if mag not in tankcfg:
        tankcfg[mag] = None

    if tankcfg[mag]: #正在测漏中
        if ret['rstatus']: #or ret['_direct']: #非静止状态
            tankcfg[mag] = None
            #print('终止测漏（2）:因收发油停止测漏')
        else:
            #时间间隔大于测漏样本时间长度
            if (ret['dtime'] - tankcfg[mag]['dtime']).total_seconds() >= 3600*params['leaksampdur']:
                sample = (ret['dtime'], ret['oilh'], ret['v20'], ret['v20']-tankcfg[mag]['v20'],
                            tankcfg[mag]['dtime'], tankcfg[mag]['oilh'], tankcfg[mag]['v20'] )
                tankcfg['_clHis'].append(sample)    #TODO:是否需要取均值

                while len(tankcfg['_clHis']) > params['leaksampcnt']:
                    tankcfg['_clHis'].popleft()

                #输出测漏记录
                tankcfg['_clRec'] = sample
                #print('输出测漏记录')

                tankcfg[mag] = None
                #print('终止测漏（0）:终止一轮测漏')
            elif ret['dtime'] < tankcfg[mag]['dtime']: #可能调过系统时间
                tankcfg[mag] = None
                #print('终止测漏（1）:当前测漏时间小于上次测漏时间')
    else:
        if ret['rstatus'] == 0 and ret['vcf'] > 0.55: #and ret['_direct']==0:
            tankcfg[mag] = ret #是否需要取均值 ？
            #print('启动测漏')

def alarm_leak(ret, tankcfg, params):
    ''' 测漏报警 '''
    #if len(tankcfg['_clHis']) > 3:
    if len(tankcfg['_clHis']) >= params['leaksampcnt'] >0: # 不该出现 >cn
        p = sum(v[3] < 0 for v in tankcfg['_clHis'])/len(tankcfg['_clHis']) * 100.
        if p >= tankcfg['leakSL']:
            ret['alarm'].append('渗漏报警')


# 采集数据，罐数据，公共参数
def processData(ret, tankcfg, params):
    tp = ret['_type']
    try:
        #校准 modbus
        if tp == 1 or (tp==2 and ret['_simu']==257):
            levelCorrect(ret, tankcfg)
            statusCode(ret, tankcfg, params)  #判断状态

        # modbus/fmu 计算密度和体积等
        if tp <= 2:
            calDen(ret, tankcfg, params)
            calVol(ret, tankcfg, params)

        #ret['alarm'] = [] #液 水 温 流 泄 渗 通
        #报警：液位/水位/温度
        alarm_product(ret, tankcfg)
        alarm_interface(ret, tankcfg)
        alarm_temperature(ret,tankcfg)

        #测漏/泄露
        if (params['leaksampcnt'] >0 and params['leaksampdur'] >0):
            subLeakSample(ret, tankcfg, params)
            alarm_leak(ret, tankcfg, params)

        #流速
        handle_flowrate(ret, tankcfg, params)

        #if ret['vcf'] > 0.55:
        if True:
            tankcfg['_his'].append(ret)
    except Exception as e:
        logging.warning(str(e), exc_info=True)


if __name__ == "__main__":
    import time
    s = time.time()
    for d in range(600,900,10):
        for t in range(0, 10):
            #print((t,d), findVCF(d, t+0.25),findD20(d, t+0.1))
            x = [(findVCF(d, t+0.25),findD20(d, t+0.1*i)) for i in range(100)]
    print(time.time() - s)

    #d = [(10, 9),(15, 13),(30, 25)]
    #for i in (8,9, 10, 13, 15, 25, 26):
    #    print(i, nlCalibrate(i, d))

    #for t in range(2,6):
    #    print(findDen(683, t-0.122, dir))