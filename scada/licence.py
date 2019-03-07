import os, rsa, uuid
import netifaces as nf
import hashlib as hl
from datetime import datetime
from base64 import b32encode, b32decode
from distutils.version import StrictVersion

#BASE32(CONCAT(PRIVATE_KEY_ENCRYPTED(HASH(DATA)), DATA))

def get_maccode():
    try:
        macaddr = nf.gateways()['default'][nf.AF_INET][1]
    except:
        pass
    finally:
        if not macaddr:
            macaddr = nf.interfaces()[0]

    macaddr = nf.ifaddresses(macaddr)[nf.AF_LINK][0]['addr'].strip('\'').replace(':', '')
    macaddr = uuid.UUID(int=int(macaddr, 16)).hex
    try:
        hdserail = uuid.uuid5(uuid.NAMESPACE_DNS, 'wyzx').hex
        return hl.md5(''.join([ii for i in zip(macaddr, hdserail) for ii in i]).encode()).hexdigest()
    except BaseException as e:
        print(e)
        return None

class LicDataError(Exception):
    def __init__(self, desc):
        super(LicDataError, self).__init__()
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


class LicDateInvalid(Exception):
    def __init__(self, desc):
        super(LicDateInvalid, self).__init__()
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


class LicItemError(Exception):
    def __init__(self, desc):
        super(LicItemError, self).__init__()
        self.desc = desc

    def __str__(self):
        return repr(self.desc)


def get_lic_info(licfile, pub_key=None, raise_exception=True):
    try:
        if raise_exception and not pub_key:
            raise LicDataError('invalid key')
        with open(licfile, 'rb') as fp:
            encdata = b32decode(fp.read())
            message, sig = encdata[256:], encdata[:256]
            #print('reg info is %s' %(message.decode(), ))
            if not raise_exception or rsa.verify(message, sig, pub_key):
                return dict([msg.split(':') for msg in message.decode().split(';')])
    except rsa.VerificationError:
        raise LicDataError('invalid key')
    except FileNotFoundError:
        raise LicDataError('no key data exist')


class LicItemMatcher:
    def __init__(self, name, locval):
        self.name = name
        self.locval = locval

    def match(self, licval, locval):
        if licval != locval:
            raise LicItemError('%s is mismatched' % (self.name, ))

    def test(self, lic):
        licval = lic.pop(self.name)
        if not licval:
            raise LicItemError('%s is missing' % (self.name, ))
        self.match(licval, self.locval)


class DateMatcher(LicItemMatcher):
    def match(self, licval, locval):
        edate = licval
        locval = locval.timestamp()
        if locval > float(edate):
            raise LicDateInvalid('licence is expired')



class VersionMatcher(LicItemMatcher):
    def match(self, licval, locval):
        if StrictVersion(locval) != StrictVersion(licval):
            raise LicItemError('version is mismatched')



def check_lic(licfile, pub_key, product, version, maccode):
    # machine code mismatched
    try:
        locrec = {
            'maccode': LicItemMatcher('maccode', maccode),
            'product': LicItemMatcher('product', product),
            'expire-date': DateMatcher('expire-date', datetime.utcfromtimestamp(
                        datetime.now().timestamp())),
            'version': VersionMatcher('version', version)}
        licrec = get_lic_info(licfile, pub_key)
        for k, v in locrec.items():
            v.test(licrec)
    except:
        raise

