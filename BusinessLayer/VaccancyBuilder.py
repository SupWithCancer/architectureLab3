from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any
from PersistanceLayer.SingletonDataBase import Singleton
import requests
from PresentationLayer.SpecificationFilter import  MaxSalary, MinSalary, VaccancyName
from flask_restful import  reqparse
import time
class VaccancyBuilder(ABC):
    @property
    @abstractmethod
    def vaccancy(self) -> None:
        pass
    @abstractmethod
    def extract_from_source(self) ->None:
        pass
    @abstractmethod
    def reformat(self) -> None:
        pass
    @abstractmethod
    def filter(self) -> None:
        pass


class Service1VaccancyBuilder(VaccancyBuilder):
    def __init__(self) -> None:
        self.reset()
    def reset(self) -> None:
        self._vaccancy = OwnVaccancy()
    @property
    def vaccancy(self) -> OwnVaccancy:
        vaccancy = self._vaccancy
        self.reset()
        return vaccancy
    def extract_from_source(self) ->None:
        self._vaccancy.set(requests.get('http://127.0.0.1:5001/search/').json())
    def reformat(self) -> None:
        pass
    def filter(self) -> None:
        self._vaccancy.filter()
class Service2VaccancyBuilder(VaccancyBuilder):
    def __init__(self) -> None:
        self.reset()
    def reset(self) -> None:
        self._vaccancy = OwnVaccancy()
    @property
    def vaccancy(self) -> OwnVaccancy:
        vaccancy = self._vaccancy
        self.reset()
        return vaccancy
    def extract_from_source(self) ->None:
        page = [0]
        page_n = 1
        while len(page) > 0:
            page = requests.get('http://127.0.0.1:5002/price-list?page=' + str(page_n)).json()
            print(len(page))
            page_n += 1
            self._vaccancy.vaccancies += page
    def reformat(self) -> None:
        full_vaccancies = []
        for row in self._vaccancy.vaccancies:
            full_vaccancies.append(requests.get('http://127.0.0.1:5002/details/'+str(row["vaccancyId"])).json())
        self._vaccancy.set(full_vaccancies)
    def filter(self) -> None:
        self._vaccancy.filter()

class OwnVaccancyBuilder(VaccancyBuilder):
    def __init__(self) -> None:
        self.reset()
        self.db = Singleton()
    def reset(self) -> None:
        self._vaccancy = OwnVaccancy()
    @property
    def vaccancy(self) -> OwnVaccancy:
        vaccancy = self._vaccancy
        self.reset()
        return vaccancy
    def extract_from_source(self) ->None:
        self._vaccancy.set(self._vaccancy.select_all_prod())
    def reformat(self) -> None:
        my_list = []
        for row in self.vaccancy.vaccancies:
            a = {"vaccancyId": row[0], "vaccancy_name": (row[1]), "description": (row[2]), "social_package": row[3], "salary": row[4]}
            my_list.append(a)
        self._vaccancy.set(my_list)
    def filter(self) -> None:
        self._vaccancy.filter()
class Director:
    def __init__(self) -> None:
        self._builder = None

    @property
    def builder(self) -> builder:
        return self._builder

    @builder.setter
    def builder(self, builder: builder) -> None:
        self._builder = builder

    def build_all_vaccancy(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()
    def build_filtered_vaccancy(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()
        self.builder.filter()
class OwnVaccancy():
    def __init__(self):
        self.vaccancies = []
        self.filtered_vaccancies = []
        self.conn = Singleton().conn
        self.args = {}
    def add(self, vaccancy: dict[str, Any]):
        self.vaccancies.append(vaccancy)
    def join(self, another_vaccancy):
        self.vaccancies += another_vaccancy.vaccancies
    def drop(self, id):
        del self.vaccancies[id]
    def set(self, vaccancies):
        self.vaccancies = vaccancies
    def select_all_prod(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT p1."vaccancyId", p1."vaccancy_name",p1."description", p1."social_package", p1."salary" FROM "vaccancy" p1')
            rows = cursor.fetchall()
        return rows

    def insert(self, args):
        with self.conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO "vaccancy" ("vaccancy_name", "description", "salary") VALUES('%s','%s','%s')''' % (
                args["vaccancy_name"], args["description"], str(args["salary"]),
                ))
        self.conn.commit()
        with self.conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO "CacheTable" ("vaccancy_name", "description", "salary") VALUES('%s','%s','%s')''' % (
                args["vaccancy_name"], args["description"], str(args["salary"]))),
        self.conn.commit()
    def delete(self, id):
        with self.conn.cursor() as cursor:
            cursor.execute('DELETE FROM "vaccancy" WHERE "vaccancyId"='+str(id))
            cursor.execute('DELETE FROM "CacheTable" WHERE "vaccancyId"=' + str(id))
        self.conn.commit()

    def update(self, args):
        query_str = 'UPDATE "vaccancy" SET '
        for key, value in args.items():
            if key != 'vaccancyId' and value !=None:
                query_str += '"' + key + '"=' + "'" + str(value) + "',"
        query_str = query_str[0:-1]
        query_str += ' WHERE "vaccancyId"=' + str(args["vaccancyId"])
        with self.conn.cursor() as cursor:
            cursor.execute(query_str)
        self.conn.commit()
        query_str = 'UPDATE "CacheTable" SET '
        for key, value in args.items():
            if key != 'vaccancyId' and value != None:
                query_str += '"' + key + '"=' + "'" + str(value) + "',"
        query_str = query_str[0:-1]
        query_str += ' WHERE "vaccancyId"=' + str(args["vaccancyId"])
        with self.conn.cursor() as cursor:
            cursor.execute(query_str)
        self.conn.commit()

    def mfilter(self, x):
        # print(len(self.filtered_products))
        vaccancy_filter =  MaxSalary() & MinSalary() & VaccancyName()
        if vaccancy_filter.is_satisfied_by(x, self.args):
            return x
        return None
    def filter(self):
        #product_filter = SaleType() & MaxPrice() & MinPrice() & ProductName()
        vaccancies = []
        parser = reqparse.RequestParser()
        parser.add_argument("vaccancy_name")
        parser.add_argument("min_salary")
        parser.add_argument("max_salary")
        self.args = parser.parse_args()
        import multiprocessing
        self.conn = None
        t1 = time.time()
        with multiprocessing.Pool(4) as pool:
            self.vaccancies = pool.map(self.mfilter, self.vaccancies)
        print(time.time()-t1)
        t1 = time.time()
        self.vaccancies = list(filter(None, self.vaccancies))
        print(time.time() - t1)
        self.conn = Singleton().conn
        #print(len(self.filtered_vaccancies))
        #for i in self.vaccancies:
        #    if vaccancy_filter.is_satisfied_by(i):
        #        vaccancies.append(i)
        #self.vaccancies = self.filtered_vaccancies

    def reform(self, row):
        return {"vaccancyId": row[0], "vaccancy_name": row[1], "description": row[2], "social_package": str(row[3])}