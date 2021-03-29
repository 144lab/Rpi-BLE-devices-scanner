from influxdb import InfluxDBClient
from bluepy.btle import Scanner, ScanEntry, DefaultDelegate
from struct import unpack
from time import time, sleep
from datetime import datetime, timezone
from collections import deque, defaultdict
from threading import Thread
from queue import Queue, Empty
from uuid import UUID

MY_SERVICE_BASE_UUID = UUID('f96d0000-1139-4e07-8ccf-d28be904fc0f')

def influxdb_writer(db,q):
    running = True
    while running:
        try:
            buckets = defaultdict(list)
            while running:
                try:
                    points, policy = q.get(timeout=1)
                    if points is None and policy is None:
                        running = False
                    else:
                        buckets[policy].append(points)
                except Empty:
                    break
            for k, v in buckets.items():
                db.write_points(v,retention_policy=k)
                logger.info('{0} {1} {2}'.format(k, len(v), v[-1]))
        except Exception as e:
            logger.exception(e)

class ScanDelegate(DefaultDelegate):
    def __init__(self, q):
        DefaultDelegate.__init__(self)
        self.q = q

    def handleDiscovery(self, dev, isNewDev, isNewData):
        # Length(1) + DATA(N)
        #             AD Type(1) + AD Data(n)
        #                          Service UUID(16) + Data(m)
        service_data = dev.getValue(ScanEntry.SERVICE_DATA_128B)
        if service_data:
            data = service_data[16:]
            n = len(data)//2
            if n>0:
                r_data = bytes(reversed(service_data[:16]))
                service_uuid = UUID(bytes=r_data[:2]+b'\x00\x00'+r_data[4:16])
                service_uuid16 = unpack(f'H', r_data[2:4])[0]
                if service_uuid == MY_SERVICE_BASE_UUID:
                    fields = unpack(f'{n}h', data)
                    logger.info('{0} {1} {2}'.format(dev.addr, service_uuid16, fields))
                    self.q.put(({
                        'measurement':'sensordata',
                        'tags':{'address':dev.addr, 'service_uuid16':service_uuid16},
                        'fields':{f'field{i}':v for i,v in enumerate(fields)},
                        'time':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')},
                        'long_term_storage_policy'))
        # manufacture = dev.getValue(ScanEntry.MANUFACTURER)
        # if manufacture:
        #     n = len(manufacture)//2
        #     data = unpack('<H'+'h'*(n-1), manufacture[:n*2])
        #     if data[0] == 0xffff:
        #         logger.info('{0} {1}'.format(dev.addr, data))
        #         fields = {}
        #         for i, v in enumerate(data[1:]):
        #             fields[f'field{i}'] = v
        #         self.q.put(({
        #             'measurement':'sensordata',
        #             'tags':{'address':dev.addr},
        #             'fields':fields,
        #             'time':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')},
        #             'long_term_storage_policy'))

def main(host, port, username, password, dbname):
    db = InfluxDBClient(host, port, username, password, dbname)
    db.create_database(dbname)
    logger.info(db.get_list_database())

    policies = [p['name'] for p in db.get_list_retention_policies()]
    if 'long_term_storage_policy' not in policies:
        db.create_retention_policy('long_term_storage_policy', '52w', 1)
    logger.info(db.get_list_retention_policies())
    done = True

    q = Queue()
    t = Thread(target=influxdb_writer, args=(db,q))
    t.start()

    scanner = Scanner().withDelegate(ScanDelegate(q))
    while True:
        try:
            devices = scanner.scan(10)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            logger.exception(e)
    q.put((None,None))
    t.join()

if __name__ == '__main__':
    import argparse
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('scanner')
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument(
        '--host', type=str, required=False, default='127.0.0.1', help='hostname of InfluxDB http API')
    parser.add_argument(
        '--port', type=int, required=False, default=8086, help='port of InfluxDB http API')
    parser.add_argument(
        '--username', type=str, required=False, default='root')
    parser.add_argument(
        '--password', type=str, required=False, default='root')
    parser.add_argument(
        '--dbname', type=str, required=False, default='SENSORDATA')
    args = parser.parse_args()

    main(args.host, args.port, args.username, args.password, args.dbname)
