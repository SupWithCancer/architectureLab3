from flask import Flask
from flask_restful import Resource, Api
import psycopg2
from DatabaseLayer.database import *
from PresentationLayer.SpecificationFilter import MaxSalary, MinSalary, VaccancyName
import time
import random
from flask_restful import  reqparse

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
class SingletonDB(metaclass=SingletonMeta):
    def __init__(self):
        self.conn = psycopg2.connect(host=host, user=user, database=db_name, password = password, port="5432")

    def select_all_prod(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT p1."vaccancyId", p1."vaccancy_name",p1."description", p1."social_package", p1."salary" FROM "vaccancy" p1')
            rows = cursor.fetchall()
        return rows




class Vaccancies(Resource):
    #parser = reqparse.RequestParser()
    def get(self):
        db = SingletonDB()
        time.sleep(random.randint(20, 30))
        all_vaccancies = db.select_all_prod()
        my_list = []
        for row in all_vaccancies:
            a = {"vaccancyId": row[0], "vaccancy_name": (row[1]), "description": (row[2]), "social_package": row[3],
                 "salary": row[4]}
            my_list.append(a)
        all_vaccancies.clear()

        vaccancy_filter = MaxSalary() & MinSalary() & VaccancyName()
        vaccancies = []
        parser = reqparse.RequestParser()
        parser.add_argument("vaccancy_name")
        parser.add_argument("min_salary")
        parser.add_argument("max_salary")
        args = parser.parse_args()
        print(args)
        for i in my_list:

            if vaccancy_filter.is_satisfied_by(i):
                vaccancies.append(i)
        return vaccancies

if __name__ == "__main__":
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Vaccancies, '/search/')
    app.run(port=5001, debug=True)