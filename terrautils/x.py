
import re
import datetime

def timestamp_to_terraref(timestamp):
    """Converts a timestamp to TERRA REF format
    Args:
        timestamp(str): the ISO timestamp to convert
    Returns:
        The ISO string reformatted to match the TERRA REF time & date stamp. Returns the original
        string if time units aren't specified
    Note:
        Does a simple replacement of the 'T' to '__' and stripping the fraction of seconds and
        timezone information from the string.
    """
    return_ts = timestamp

    if 'T' in timestamp:
        regex_mask = r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})'
        res = re.search(regex_mask, timestamp)
        if res:
            return_ts = res.group(0)
            return_ts = return_ts.replace('T', '__').replace(':', '-')

    return return_ts 

r = [r'(\d{4}(/|-){1}\d{1,2}(/|-){1}\d{1,2}__\d{2}-\d{2}-\d{2}[\-{1}\d{3}]*)',
     r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})[+-](\d{2})\:(\d{2})',
     r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})(.\d+)[+-](\d{2})\:(\d{2})',
     r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})(.\d+)',
     r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})',
     r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})Z'
    ]
s = ["PSII PNGs - 2018-06-22__05-35-30-998_100",
    "PSII PNGs - 2018-06-22__05-35-30",
    "PSII PNGs - 2018-06-22",
    "2018-12-12T22:03:51-04:00",
    "2018-12-12T22:03:51+04:00",
    "2018-12-12T22:03:51Z",
    "2019-04-29T14:49:35.491133+01:00",
    "2019-04-29T14:49:35",
    "2019-04-29T14:49:35."
]

for sp in s:
    print "> " + sp
    for part in sp.split(' - '):
         for form in r:
            res = re.search(form, part)
            if res:
                print timestamp_to_terraref(res.group(0))
                break

ZERO = datetime.timedelta(0)
class TZ(datetime.tzinfo):
    def utcoffset(self, dt): return ZERO
    def dst(self, dt): return ZERO
print "ISO: " + datetime.datetime.now(tz = TZ()).isoformat()






