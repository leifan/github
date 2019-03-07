# encoding:utf-8

from urllib.parse import quote
from itertools import groupby
import json, logging

from sqlalchemy import create_engine, MetaData
from datetime import timedelta
import time

def dbOutput(info, newdata, static=[(None,)]):
    try:
        if not info['enable'] or not newdata:
            return False

        tdelta = timedelta(seconds=-time.timezone)

        hotrd, hisrd = info['hottblname'], info['histblname']
        tnames = [tn for tn in (hotrd, hisrd) if tn]

        if info != static[0][0]: #重连
            uid, pwd, dsn = info['username'], info['password'], info['dbname']
            engine = create_engine("mssql+pyodbc://{}:{}@{}".format(uid, quote(pwd), dsn))
            meta = MetaData()
            meta.reflect(engine)

            HisRd = meta.tables.get(hisrd, None)
            HotRd = meta.tables.get(hotrd, None)
            fmap = json.loads( info['fieldmap'] )
            fmap = {k:v for k,v in fmap.items() if v}

            cnstmap = {}
            if 'constantmap' in info and info['constantmap']:
                cnstmap = json.loads(info['constantmap'])

            static[0] = info, engine, HisRd, HotRd, fmap, cnstmap
        else:
            _, engine, HisRd, HotRd, fmap, cnstmap = static[0]

        #newdata = [r.copy() for r in data]
        stcodes = ['静止', '进油', '出油', '静止']
        for r in newdata:
            r['rstatus'] = stcodes[r['rstatus'] & 0x03]
            r['alarm'] = ','.join(r['alarm']) if r['alarm'] else '无报警'
            r['dtime'] = r['dtime'].replace(tzinfo=None) + tdelta #local
            r['owvol'] = r['oilvol'] + r['watervol']
            for k, v in cnstmap.items():
                if k and v:
                    r[k] = v

            r.update([(v,r[k]) for k,v in fmap.items() if k in r])

        #处理热点数据
        kf = lambda x: x['tk_id']
        hots = [max(g, key=lambda x:x['dtime']).copy()
                for k,g in groupby(sorted(newdata, key=kf), key=kf)]

        try:
            conn = engine.connect()
            if HisRd != None:
                pkcols = [c.name for c in HisRd.primary_key.columns]
                destcols = {c.name:c.type.python_type for c in HisRd.columns}
                
                for r in newdata:
                    kick = [r.pop(k, None) for k in list(r.keys()) if k not in destcols]
                    r.update((k, destcols[k]()) for k in destcols if k not in r and k not in pkcols)

                conn.execute(HisRd.insert(), newdata)

            if HotRd != None:
                pkcols = [c.name for c in HotRd.primary_key.columns]
                destcols = {c.name:c.type.python_type for c in HotRd.columns}

                idname = fmap.get('tk_id', 'tk_id')
                ids = [r[idname] for r in conn.execute(HotRd.select())]
                for r in hots:
                    kick = [r.pop(k, None) for k in list(r.keys()) if k not in destcols]
                    r.update((k, destcols[k]()) for k in destcols if k not in r and k not in pkcols)

                    if r[idname] in ids:
                        conn.execute(HotRd.update().values(**r).where(HotRd.c[idname] == r[idname]))
                    else:
                        conn.execute(HotRd.insert().values(**r))
            
            #conn.commit()
            logging.debug('数据写入第三方数据库')
        except Exception as e:
            logging.warn('第三方数据库写入失败: ' + str(e))
            
            engine.dispose()
            static[0] = (None,) #cause reconnect
        finally:
            conn.close()
    except Exception as e:
        logging.warn(str(e))


if __name__ == '__main__':
    from datetime import datetime
    info = {'dbname':'wyzx1', 'username':'sa', 'password':'asdf123@4',
            'histblname':'runningdata1', 'hottblname':'hotrd', 'enable':True,
            'fieldmap':'{"v20":"v20"}'}
    data = [{'tk_id':146, 'v20':109, 'dtime':datetime.now(), 'rstatus':2, 'alarm':['水位高报']}]
    
    #test(info, [{'tk_id':144, 'v20':101, 'dtime':datetime.now(), 'rstatus':"aa", 'alarm':'hah'}])
    dbOutput(info, data)
