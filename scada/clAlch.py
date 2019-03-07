# encoding:utf-8
import time
from datetime import datetime, timezone
import os, sqlite3

import logging
from logging.handlers import RotatingFileHandler
from threading import Thread, Event
from queue import Queue, Empty
from collections import deque, defaultdict
from itertools import groupby

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, subqueryload, joinedload
from sqlalchemy import create_engine, MetaData
from sqlalchemy import exc as SaExc

from proto import MetaRegCls
import oilcomp as oc
import dbEx, checklic


# 采集线程N
# 需要处理接口协议，多客户端轮询，单客户查询
class Collector(Thread):
    def __init__(self, qData, ch, addrs, period, qPresDeq, space=0.05):
        super().__init__(daemon=True)
        self.finished = Event()
        self.qData = qData
        self.ch = ch
        self.addrs = addrs
        self.interval = period
        self.qPresDeq = qPresDeq
        self.space = space

    def stop(self):
        self.finished.set()

    def run(self):
        logging.info('通道 %s 启动:\n\t %s'%(self.ch.portName(),
                "\n\t".join(str(e) for e in self.addrs)))
        while not self.finished.is_set():
            t0 = time.time()

            for tkid,_,*a in self.addrs:
                time.sleep(self.space)
                ret = self.ch.getSlaveData(*a)

                if ret:
                    current = datetime.now(tz=timezone.utc)
                    if self.qData is self.qPresDeq: #压力数据
                        self.qPresDeq[tkid].append((current, ret))
                    else: #液位数据
                        #if len(ret) <= 8: #直连，查时间最近的压力数据
                        if ret['_type'] == 1:
                            press = 0
                            if self.qPresDeq[tkid]:
                                pd, press = min(self.qPresDeq[tkid], key=lambda x: abs(current-x[0]))
                                if abs((current-pd).total_seconds()) > 10.0: #超过10s没有压力数据
                                    press = 0

                            #if press == 0:
                            #    logging.warning('压力数据丢失')
                            ret['pressvalue'] = press
                        ret.update(tk_id=tkid, dtime=current, alarm=[])
                        self.qData.put(ret)
                else:
                    logging.warning('{} {} {}'.format(self.ch.portName(), '通信故障', a))

            left = self.interval - (time.time() - t0)
            if left > 0.1:
                time.sleep(left)
            logging.debug('%s轮询用去%.3f秒'%(self.ch.portName(), self.interval-left))

        self.ch.close()
        logging.info('通道 %s 关闭'%self.ch.portName())


class localBuf():
    def __init__(self, fn, colstype=None):
        self.fn = fn
        self.tname = 'runningdata'

        try:
            conn = sqlite3.connect(self.fn)
            conn.row_factory = sqlite3.Row
            self.cur = conn.cursor()

            py2sqlite = {int:'INTEGER', float:'REAL', datetime:'TEXT', str:'TEXT'}
            sqlite2py = {v:k for k,v in py2sqlite.items()}
            if colstype:
                colstr = ','.join('{} {}'.format(k, py2sqlite[v]) for k,v in colstype.items())
                self.colstype = colstype
            else:
                colstr = '''dtime TEXT,oilh REAL,waterh REAL,avg_temp REAL,temp1 REAL,temp2 REAL,
                temp3 REAL,temp4 REAL,temp5 REAL,pressvalue REAL,orig_oilh REAL,oilvol REAL,emptyvol REAL,
                v20 REAL,watervol REAL,mass REAL,flowrate REAL,tk_id INTEGER,rstatus TEXT,cdensity REAL,
                dencomp REAL,nolinercomp REAL,jzden REAL,sden REAL,vcf REAL,alarm TEXT'''
                colstype = [c.split() for c in colstr.split(',')]
                self.colstype = {c[0]:sqlite2py[c[1]] for c in colstype}

            csql = 'create table if not exists {}({})'.format(self.tname, colstr)
            self.cur.execute(csql)

            #self.colnames = [c.split()[0] for c in colstr.split(',')]
            self.insql = "insert into {}({}) VALUES({})".format(self.tname,
                ','.join(self.colstype.keys()), ','.join(':%s'%cn for cn in self.colstype.keys()))

            self.cond = ' where 0 < 1'
        except Exception as e:
            logging.warning(str(e))
            self.cur = None

    def save(self, data):
        try:
            for d in data:
                d.update((k, self.colstype[k]()) for k in self.colstype.keys() if k not in d)

            self.cur.executemany(self.insql, data)
            self.cur.connection.commit()
        except Exception as e:
            logging.warning(str(e))

    def read(self, page=512):
        try:
            mrsql = "select rowid from {} order by rowid asc limit 1 offset ?".format(self.tname)
            self.cur.execute(mrsql, (page,))
            maxrow = self.cur.fetchone()
            self.cond = ' where rowid < {}'.format(maxrow[0]) if maxrow else ""

            selsql = 'select * from {} {}'.format(self.tname, self.cond)
            self.cur.execute(selsql)
            records = self.cur.fetchall()

            return records
        except Exception as e:
            logging.warning(str(e))

        return []

    def vacuum(self):
        delsql = 'delete from {} {}'.format(self.tname, self.cond)
        while True:
            try:
                self.cur.execute(delsql)
                self.cur.connection.commit()
            except Exception as e:
                logging.warning(str(e))
            else:
                break

    def count(self):
        try:
            self.cur.execute('select count(*) from %s' % self.tname)
            ret = self.cur.fetchone()[0]
            return ret
        except Exception as e:
            logging.warning(str(e))

        return 0


# 写入线程1
# 数据处理，及时报警
class Writer(Thread):
    def __init__(self, qData, interval, engine, Base, tanks, params):
        super().__init__(daemon=True)
        self.finished = Event()
        self.qData = qData
        self.interval = interval
        self.engine = engine
        self.Base = Base

        self.tanks = tanks
        self.params = params

        self.session = None

    def stop(self):
        self.finished.set()

    def loadSession(self):
        try:
            #engine = self.engine # create_engine(self.info, poolclass=NullPool, echo=0)
            #tnames = ['ots_runningdata', 'ots_hotrd', 'ots_oilinreport',
            #        'ots_leakrecord', 'ots_alarmdata'] #, 'ots_tank', 'ots_tankzone']
            #meta = MetaData()
            #meta.reflect(engine, only=tnames)
            #Base = automap_base(metadata=meta)
            #Base.prepare()

            session = Session(bind=self.engine)

            rdHolder = {c.name:c.type.python_type for c in
                        self.Base.classes.ots_hotrd.__table__.columns}

            self.session = session
            self.rdHolder = rdHolder
        except Exception as e:
            logging.debug(str(e), exc_info=True)
            self.session = None

    def run(self):
        self.loadSession()
        tanksnum = len(self.tanks)

        bufn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'buff.db')
        self.sqlBuf = localBuf(bufn, self.rdHolder if self.session else None)
        buffered = self.sqlBuf.count() > 0

        data  = []
        stcodes = ['静止', '进油', '付油', '静止']
        while not self.finished.is_set():
            tt0 = time.time()

            try:
                soundAlarms(self.session, self.Base) #声光报警
            except Exception as e:
                logging.warn(str(e))

            #获取采集数据
            try:
                while len(data) < tanksnum:
                    #block only for the first 4
                    r = self.qData.get(block = len(data)<3, timeout=self.interval/4)

                    tankcfg = self.tanks[r['tk_id']]
                    r['tk__number'] = tankcfg['number']
                    r['tk__safeVol'] = tankcfg['safeVol']
                    r['tk__oils_name'] = tankcfg['oil_name']
                    r['tk__oils_code'] = tankcfg['oil_code']

                    if False:
                        try:
                            t = r['dtime']
                            sign = t.minute%4
                            delta = (1-(sign<2))*60 + (sign%2)*(2-sign)*(t.second + t.microsecond/1000000)
                            r['oilh'] = tankcfg['height'] * 0.5 + delta*2
                            r['waterh'] = 20 + delta/5.0 #- delta/25 + (t.second%4)/40.
                            r['pressvalue'] = tankcfg['density']*9.8*(r['oilh'] - tankcfg['_ps']['location'])/1000000
                            r['avg_temp'] = 20 + (1 if t.hour//12 else -1)*(t.hour%12 + t.minute/60.)
                        except Exception as e:
                            logging.warn(str(e), exc_info=True)

                    oc.processData(r, tankcfg, self.params)
                    #print(r['oilh'], r['waterh'], stcodes[r['rstatus'] & 0x03], r['alarm'], r['flowrate'])

                    data.append(r)
            except Empty:
                pass

            newdata = [r.copy() for r in data] #for 3rd parth

            bFull = len(data) == tanksnum
            #尝试写Remote
            if data:
                try:
                    if not self.session:
                        self.loadSession()
                    if self.session:
                        destcols = self.rdHolder
                        #数据整理好，rstatus和alarm例外
                        for r in data:
                            kick = [r.pop(k, None) for k in list(r.keys()) if k not in destcols]
                            r.update((k, destcols[k]()) for k in destcols if k not in r)

                            hd = self.Base.classes.ots_runningdata(**r)
                            hd.rstatus = stcodes[ r['rstatus'] & 0x03 ]
                            hd.alarm = ','.join(r['alarm'])
                            self.session.add(hd)

                            tankcfg = self.tanks[r['tk_id']]
                            #写报警记录
                            ADT = self.Base.classes.ots_alarmdata
                            lastAlarms = tankcfg['_his'][-2]['alarm'] if len(tankcfg['_his'])>1 else []
                            for alert in lastAlarms:
                                if alert not in r['alarm']:
                                    arec = self.session.query(ADT).filter(ADT.tk_id==r['tk_id'], ADT.name==alert,
                                         ADT.etime==None).order_by(ADT.stime.desc()).first()
                                    if arec:
                                        arec.etime = r['dtime']
                                        #print(alert, '结束报警')
                            for alert in r['alarm']:
                                if alert not in lastAlarms:
                                    ad = self.Base.classes.ots_alarmdata(
                                        stime=r['dtime'], name=alert, tk_id=r['tk_id'])
                                    self.session.add(ad)
                                    #print(alert, '开始报警')

                            #写测漏记录
                            clr = tankcfg.get('_clRec', None)
                            if clr:
                                clRec = self.Base.classes.ots_leakrecord(tank_id=r['tk_id'],
                                    eDtime=clr[0] ,eOilh=clr[1], eV20=clr[2], v20dlt=clr[3],
                                    sDtime=clr[4] ,sOilh=clr[5], sV20=clr[6])
                                tankcfg['_clRec'] = None
                                self.session.add(clRec)
                                #logging.debug('写测漏记录', clRec)

                            #写进油记录
                            nRecs = len(tankcfg['_oilinRec']) // 2
                            if nRecs:
                                mAps = ['Dtime','Oilh','Waterh','Avg_temp','OilVol','WaterVol','V20']
                                       #     ['dtime','oilh','waterh','avg_temp', 'oilvol','watervol','v20'])
                                for i in range(nRecs):
                                    ts,te = tankcfg['_oilinRec'][2*i:2*i+2]
                                    if ( abs((ts['dtime'] - te['dtime']).total_seconds()) < self.params['minlasttime']*60
                                       or abs(ts['oilh'] - te['oilh']) < self.params['minhchange'] ):
                                        continue  #过滤不符合条件的进油记录

                                    dd = {'tank_id':r['tk_id'], 'oils':tankcfg['oil_name'], 'zone':tankcfg['zone_name']}
                                    for prefix, rtemp in zip('se', tankcfg['_oilinRec'][2*i:2*i+2]):
                                        for suf in mAps:
                                            dd[prefix+suf] = rtemp[suf.lower()]

                                    oilinRec = self.Base.classes.ots_oilinreport(**dd)
                                    self.session.add(oilinRec)
                                    #logging.debug('写进油报告')
                                tankcfg['_oilinRec'][:2*nRecs] = []

                        #处理热点数据
                        kf = lambda x: x['tk_id']
                        hots = [max(g, key=lambda x:x['dtime'])
                                for k,g in groupby(sorted(data, key=kf), key=kf)]
                        for r in hots:
                            hd = self.Base.classes.ots_hotrd(**r)
                            hd.rstatus = stcodes[ r['rstatus'] & 0x03 ]
                            hd.alarm = ','.join(r['alarm'])
                            newhd = self.session.merge(hd)
                            self.session.add(newhd)

                        self.session.commit()

                        self.session.expunge_all()
                        logging.debug('主库写入{}条记录'.format(len(data)))
                        data[:] = [] #empty the list
                except Exception as e:
                    self.session.close()
                    self.session = None
                    logging.warning(str(e), exc_info=True)

            #未能写入remote，写入localbuf
            if data:
                trand = []
                for r in data:
                    hd = dict(r)
                    hd['rstatus'] = stcodes[ r['rstatus'] & 0x03 ]
                    hd['alarm'] = ','.join(r['alarm'])
                    trand.append(hd)

                self.sqlBuf.save(trand)
                buffered = True
                logging.debug('本地缓存 {} 条数据'.format(len(data)))
                data[:] = []

            if self.session and buffered:
                try:
                    records = self.sqlBuf.read()
                    if records:
                        for r in records:
                            hd = self.Base.classes.ots_runningdata(**r)
                            self.session.add(hd)
                        self.session.commit()

                        self.sqlBuf.vacuum()
                        logging.debug('{} 条缓存数据写入主库'.format(len(records)))
                    else:
                        buffered = False
                except SaExc.IntegrityError:
                    self.sqlBuf.vacuum()
                    logging.debug('{} 条缓存数据丢弃'.format(len(records)))
                except Exception as e:
                    logging.warning(str(e), exc_info=True)

            #写第三方
            if newdata:
                dbEx.dbOutput(self.params['_dbEx'], newdata)

            left = self.interval - (time.time() - tt0)
            if (not bFull) and (not buffered) and (left > 0.2):
                time.sleep(left)    #礼让其他线程
            logging.debug('Writer用去{:.4f}秒'.format(self.interval-left))

        logging.info('Writer 结束')


def getBasic(engine, Base, gvol=True, resetD=False):
    try:
        session = Session(bind=engine)

        Tank = Base.classes.ots_tank
        Channel = Base.classes.ots_channel
        Volumn = Base.classes.ots_volumntable
        Cali =  Base.classes.ots_liquidmodify

        #通道
        channels =[]
        for c in session.query(Channel).all():
            if c.devprotcl.lower() == 'hart':
                channels.insert(0, (c.port, c.devprotcl.lower(), c.period,
                    [(ps.tk_id, ps.channel_id, ps.manuIDCode,ps.devTypeCode,ps.devIDCode)
                    for ps in c.ots_pressgauge_collection if ps.isused],
                    c.timeout, c.interval))
            else:
                channels.append((c.port, c.devprotcl.lower(), c.period,
                    [(lg.tk_id, lg.channel_id, lg.firstAddr, lg.secondAddr)
                    for lg in c.ots_liquidgauge_collection if lg.isused],
                    c.timeout, c.interval))

        #参数
        ps = (session.query(Base.classes.ots_oilinsetting).first(),
                    session.query(Base.classes.ots_generalsetting).first())
        params = dict(ps[0].__dict__) if ps[0] else {}
        params.update(dict(ps[1].__dict__) if ps[1] else {})
        params.pop('_sa_instance_state', None)

        dbEx = session.query(Base.classes.ots_dboutputsetting).first()
        dictDbEx = dict(dbEx.__dict__) if dbEx else {}
        dictDbEx.pop('_sa_instance_state', None)
        params['_dbEx'] = dictDbEx

        #储罐
        tanks = {}
        for c in ( session.query(Tank).filter(Tank.ots_liquidgauge_collection.any())
                   .options(subqueryload(Tank.ots_liquidgauge_collection))
                   .options(subqueryload(Tank.ots_pressgauge_collection))
                   .options(joinedload(Tank.ots_oils))
                   .options(joinedload(Tank.ots_tankzone))
                   ):
            rowdict = dict(c.__dict__)
            for k in ['_sa_instance_state', 'ots_liquidgauge_collection','ots_pressgauge_collection',
                        'ots_oils','ots_tankzone']:
                rowdict.pop(k, None)
            for k in rowdict:
                if rowdict[k] is None:
                    rowdict[k] = 0.0

            rowdict['zone_name'] = c.ots_tankzone.name
            rowdict['oil_name'] = c.ots_oils.name
            rowdict['oil_code'] = c.ots_oils.code

            if gvol:
                vols = list(session.query(Volumn.height, Volumn.volumn).filter(Volumn.tk_id == c.id).order_by(Volumn.height))
                if vols and vols[0][0]:
                    vols.insert(0,(0,0))
                rowdict['_vols'] = vols

            calis = list(session.query(Cali.manualOilH, Cali.manualDen, Cali.meterOilH, Cali.posDelta)
                    .filter(Cali.tank_id == c.id).order_by(Cali.meterOilH))
            rowdict['_nolinH'] = [(v[0],v[2]) for v in calis]
            rowdict['_nolinP'] = [(v[3],v[2]) for v in calis]

            rowdict['_his'] = deque(maxlen=20) #工况
            rowdict['_clHis'] = deque() #测漏样本
            rowdict['_oilinRec'] = [] #进油记录

            lg = c.ots_liquidgauge_collection[0] # have one
            ps = c.ots_pressgauge_collection[0] if c.ots_pressgauge_collection else None

            rowdict['_lg'] = dict(lg.__dict__)
            rowdict['_lg'].pop('_sa_instance_state', None)

            rowdict['_ps'] = dict(ps.__dict__) if ps else {}
            rowdict['_ps'].pop('_sa_instance_state', None)

            tanks[c.id] = rowdict

        #将报警记录终结
        if resetD:
            AlarmData = Base.classes.ots_alarmdata
            current = datetime.now(tz=timezone.utc)
            for c in session.query(AlarmData).filter(AlarmData.etime == None):
                c.etime = current
            session.commit()

        session.close()
        return channels, tanks, params
    except Exception as e:
        logging.warning(str(e), exc_info=False)

    return [],{},{}


def soundAlarms(session, Base, Chs=[]):
    if not Chs: #static
        Chs.append(None)

    SLAlarm = Base.classes.ots_soundlightalarm
    almCfg = session.query(SLAlarm).filter(SLAlarm.isused.is_(True)).first()
    if almCfg:
        if Chs[0] and Chs[0].portName() != almCfg.port:
            Chs[0].close()
            Chs[0] = None

        if not Chs[0]:
            scls = MetaRegCls.getClass('brHTACA1.0') #almCfg.devtype
            newCh = scls(almCfg.port, 1.0)
            if newCh.master:
                Chs[0] = newCh
                logging.info('声光报警通道初始化成功')

        if Chs[0]:
            if hasattr(almCfg, 'istest') and almCfg.istest:
                Chs[0].allSet(True)
                logging.debug('测试声光报警')
                return

            almSet = set()
            AlarmData = Base.classes.ots_alarmdata
            for c in session.query(AlarmData).filter(AlarmData.etime == None).filter(AlarmData.ctime == None):
                almSet.add((c.tk_id, c.name))

            setupSet = set()
            SLSetup = Base.classes.ots_soundlighttanksetup
            for c in session.query(SLSetup):
                setupSet.add((c.tank_id, c.alarmtype))

            #print(almSet, setupSet)
            if (almSet & setupSet):
                Chs[0].allSet(True)
                logging.debug('开启声光报警')
            else:
                Chs[0].allSet(False)
                logging.debug('关闭声光报警')

class WyDac():
    def __init__(self, info):
        self.info = info

        try:
            engine = create_engine(info, echo=False)
            hBase = automap_base()
            hBase.prepare(engine, reflect=True)

            self.engine = engine
            self.hBase = hBase
        except Exception as e:
            self.engine = None
            self.hBase = None

        self.threads = []
        self.qData = None
        self.pressData = None

    def startDac(self):
        engine, Base = self.engine, self.hBase
        channels, tanks, params = getBasic(engine, Base, True, True)

        if not channels or not tanks or self.threads: #不可重复执行
            return [], None, None

        qData = Queue(maxsize=0)                          #液位计数据
        pressData = defaultdict(lambda: deque(maxlen=20)) #压力数据

        threads = []
        for p in channels: # (com1, hart, 1800, [()], 800, 20)
            chCls = MetaRegCls.getClass(p[1])
            if chCls and p[3]:
                ch = chCls(p[0], p[4]/1000.)
                if ch.master:
                    dataPool = pressData if p[1] == 'hart' else qData
                    cltor = Collector(dataPool, ch, p[3], p[2]/1000., pressData, p[5]/1000.)
                    threads.append(cltor)

        addrss = set(addr[0] for t in threads for addr in t.addrs)
        logging.info("共加载{}个通道，{}个储罐".format(len(threads), len(addrss)))

        for t in threads:
            t.start()

        #if threads:
        if True:
            writer = Writer(qData, 2.0, engine, Base, tanks, params)
            threads.append(writer)
            writer.start()

        self.threads = threads
        self.qData = qData
        self.pressData = pressData

        return threads, qData, pressData

    def endDac(self):
        for t in self.threads: t.stop()
        for t in self.threads: t.join(t.interval)
        self.threads = []


    def monitor(self):
        if not checklic.is_lic_verified():
            logging.warning('软件超过试用期')
            self.endDac()
            return False
            #raise KeyboardInterrupt

        engine, Base, threads, qData, pressData = (self.engine, self.hBase,
            self.threads, self.qData, self.pressData )

        channels, tanks, params = getBasic(engine, Base, False, False)

        if not channels or not tanks:
            logging.info('获取主库配置信息失败')
            return False

        if not all(t.is_alive() for t in threads):
            logging.warn('存在死亡线程')

        #return None

        #关闭配置中停用的通道
        keeped = []
        for t in threads[:-1]:
            for newc in channels:
                if (t.ch.portName().lower() == newc[0].lower() and
                    t.ch.wyprotocal.lower() == newc[1].lower()):

                    keeped.append(t)
                    break
            else:
                t.stop()
                t.join(5.0)
        threads[:-1] = keeped

        # collector 更新 通道、设备、周期
        for p in channels: # (com1, hart, 1800, [()])
            for t in threads[:-1]:
                if t.ch.portName().lower() == p[0].lower():
                    if t.addrs != p[3]:
                        t.addrs = p[3]
                    if t.interval * 1000 != p[2]:
                        t.interval = p[2]/1000.

                    break
            else:
                chCls = MetaRegCls.getClass(p[1])
                if chCls and p[3]:
                    ch = chCls(p[0], p[4]/1000.)
                    if ch.master:
                        dataPool = pressData if p[1] == 'hart' else qData
                        collector = Collector(dataPool, ch, p[3], p[2]/1000., pressData, p[5]/1000.)
                        collector.start()
                        threads.insert(-1, collector)

        #writer 更新tanks、 params等参数
        writor = threads[-1]
        for tid in tanks:
            if tid in writor.tanks:
                #更新储罐参数
                keyskip = ['_his', '_clHis', '_oilinRec']
                for key in tanks[tid]:
                    if key not in keyskip:
                        if writor.tanks[tid][key] != tanks[tid][key]:
                            writor.tanks[tid][key] = tanks[tid][key]
            else:
                writor.tanks[tid] = tanks[tid]  #增加储罐
                logging.info('加载新储罐%s' % tanks[tid]['number'])
        writor.params = params

        return True

if __name__ == '__main__':
    import sys, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", '--server', default='localhost',
                        help="database host")
    parser.add_argument("-v", "--verbose", default=2, type=int,
                        help="logging level")
    args = parser.parse_args()

    vLevel = logging.ERROR
    if args.verbose == 1:
        vLevel = logging.WARNING
    elif args.verbose == 2:
        vLevel = logging.INFO
    elif args.verbose >= 3:
        vLevel = logging.DEBUG

    logger = logging.getLogger()
    stdHandler = logging.StreamHandler(sys.stdout)
    stdHandler.setLevel(logging.DEBUG)
    logger.addHandler(stdHandler)
    logger.setLevel(vLevel)

    LOG_FORMAT = '%(asctime)s %(filename)s:%(lineno)d %(levelname)-8s: %(message)s'
    formatter = logging.Formatter(LOG_FORMAT)
    log_filename = os.path.join(os.path.dirname(__file__), 'ewyzx.log')
    fHandler = RotatingFileHandler(log_filename, maxBytes=10*2**20, encoding='cp936', backupCount=1)
    fHandler.setFormatter(formatter)
    fHandler.setLevel(logging.WARNING)
    logger.addHandler(fHandler)

    info = 'postgresql://ots:ots2017@%s:5432/wyzx' % (args.server)

    dac = WyDac(info)
    dac.startDac()
    try:
        while True:
            time.sleep(15.0)
            dac.monitor()
    except KeyboardInterrupt:
        dac.endDac()
