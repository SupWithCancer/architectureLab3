import datetime
import threading
import multiprocessing as mp

from flask_restful import reqparse

from PersistanceLayer.SingletonDataBase import Singleton
from BusinessLayer.VaccancyBuilder import Director, Service1VaccancyBuilder, Service2VaccancyBuilder, OwnVaccancyBuilder
from psycopg2.extras import execute_values

class SingletonCache(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
class CacheVaccancy(metaclass=SingletonCache):
    def __init__(self):
        self.own_cache = []
        self.service_1_cache = []
        self.service_2_cache = []
    def time_to_update(self):
        dt = datetime.datetime.now()
        tomorrow = dt + datetime.timedelta(days=1)
        return (datetime.datetime.combine(tomorrow, datetime.time.min) - dt).seconds
    def own_prod(self, q):
        director = Director()
        builder = OwnVaccancyBuilder()
        director.builder = builder
        director.build_all_vaccancy()
        own = builder.vaccancy
        print(len(own.vaccancies))
        q.put(own.vaccancies)

    def serv1_prod(self, q):
        director = Director()
        builder = Service1VaccancyBuilder()
        director.builder = builder
        director.build_all_vaccancy()
        serv1 = builder.vaccancy
        print(len(serv1.vaccancies))
        q.put(serv1.vaccancies)
    def serv2_prod(self, q):
        director = Director()
        builder = Service2VaccancyBuilder()
        director.builder = builder
        director.build_all_vaccancy()
        serv2 = builder.vaccancy
        print(len(serv2.vaccancies))
        q.put(serv2.vaccancies)
    def update(self):
        conn = SingletonDB().conn
        q1 = mp.Queue()
        p1 = mp.Process(target=self.own_prod, args=(q1,))

        q2 = mp.Queue()
        p2 = mp.Process(target=self.serv1_prod, args=(q2,))

        q3 = mp.Queue()
        p3 = mp.Process(target=self.serv2_prod, args=(q3,))
        p1.start()
        p2.start()
        p3.start()
        self.own_cache = q1.get()
        self.service_1_cache = q2.get()
        self.service_2_cache = q3.get()
        with conn.cursor() as cursor:
            cursor.execute('TRUNCATE CacheTable')
            execute_values(cursor,
                           '''INSERT INTO CacheTable ("vaccancyId", "vaccancy_name", "desciption", "salary", "social_package" ) VALUES %s''',
                            [(args["vaccancyId"], args["vaccancy_name"], args["desciption"], str(args["salary"]), str(args["social_package"])) for args in self.own_cache + self.service_1_cache +self.service_2_cache])
        conn.commit()
        p1.join()
        p2.join()
        p3.join()

        timer = threading.Timer(self.time_to_update(), self.update)
        timer.start()
    def get_cache(self) -> object:
        parser = reqparse.RequestParser()
        parser.add_argument("vaccancy_name")
        parser.add_argument("min_salary")
        parser.add_argument("max_salary")
        args = parser.parse_args()
        parse_str = '''SELECT * FROM "CacheTable" '''
        filt_opt = []
        if args['vaccancy_name']:
            filt_opt.append(['"vaccancy_name"=', args['vaccancy_name']])
        if args['min_salary']:
            filt_opt.append(['"salary">', args['min_salary']])
        if args['max_salary']:
            filt_opt.append(['"salary"<', args['max_salary']])
        if len(filt_opt)>0:
            parse_str+='WHERE '
        for i in range(len(filt_opt)):
            parse_str += filt_opt[i][0]+"'"+ filt_opt[i][1] + "'"
            if i+1 < len(filt_opt):
                parse_str+=' AND '
        conn = Singleton().conn
        with conn.cursor() as cursor:
            cursor.execute(parse_str)
            rows = cursor.fetchall()
        result = []
        for row in rows:
            a = {"vaccancyId": row[0], "vaccancy_name": (row[1]), "description": (row[2]), "social_package": row[3],
                 "salary": row[4]}
            result.append(a)
        return result