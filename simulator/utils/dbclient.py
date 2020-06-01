from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pymongo
import hashlib
import pickle
import os
from datetime import datetime

mongohost = os.environ.get('MONGOHOST', 'localhost')
config = dict(host=mongohost,
              port=3002,
              username='admin',
              password=hashlib.md5(b'opyata').hexdigest())


class DataManagerException(Exception):
    pass


class DataManager:
    def __init__(self):
        try:
            self.client = MongoClient(**config)
            self.simulations_db = self.client.simulations
            self.db_names = {"simulations": self.simulations_db}
        except Exception as e:
            raise DataManagerException("Unsuccessful connection to MongoDB - {}. "
                                       "Check if docker is alive by command 'docker ps | grep didi_collector' or set"
                                       "write_simulations_to_db=False".format(e))

    def __call__(self, simulation_name):
        self.simulations_name = simulation_name
        self.create_simulation(simulation_name)

    def show_collections(self, db_name):
        return self.db_names[db_name].list_collection_names()

    def simulation_exists(self, simulation_name):
        return simulation_name in self.simulations_db.list_collection_names()

    def create_simulation(self, simulation_name):
        if self.simulation_exists(simulation_name):
            simulation_name += f"_{datetime.now}"  # TODO: send sim name back to connector
            if self.simulation_exists(simulation_name):
                raise DataManagerException("{} simulation already exists".format(simulation_name))
        else:
            simulation = self.simulations_db[simulation_name]
            simulation.create_index([('step', pymongo.ASCENDING)], unique=True)
            print('{} created successfully'.format(simulation_name))

    def write_simulation_step(self, step_data: dict, simulation_name=None):
        if self.simulations_name:
            simulation_name = self.simulations_name
        if not self.simulation_exists(simulation_name):
            raise DataManagerException(f"{simulation_name} does not exist. Please, use create_simulation method first")
        if 'step' not in step_data.keys():
            raise DataManagerException("Step data should contain 'step' key")
        document = self._prepare_document(step_data, read_mode=False)
        try:
            self.simulations_db[simulation_name].insert_one(document)
        except DuplicateKeyError:
            raise DataManagerException(f"Step {step_data['step']} has been already written in {simulation_name}")

    def write_simulation(self, simulation_name, simulation_data: list):
        if not self.simulation_exists(simulation_name):
            raise DataManagerException(f"{simulation_name} does not exist. Please, use create_simulation method first")
        self.simulations_db[simulation_name].insert_many(simulation_data)

    def read_simulation_step(self, simulation_name, n_step):
        if not self.simulation_exists(simulation_name):
            raise DataManagerException("{} simulation does not exist".format(simulation_name))
        result = self.simulations_db[simulation_name].find_one({'step': n_step})
        if result is None:
            raise DataManagerException(f"Step №{n_step} does not exist in {simulation_name} simulation")
        else:
            return self._prepare_document(result)

    def read_simulation(self, simulation_name):
        if not self.simulation_exists(simulation_name):
            raise DataManagerException("{} simulation does not exist".format(simulation_name))
        all_steps = self.simulations_db[simulation_name].find()
        return [self._prepare_document(step) for step in all_steps]

    def drop_simulation(self, simulation_name):
        if not self.simulation_exists(simulation_name):
            raise DataManagerException("{} simulation does not exist".format(simulation_name))
        self.simulations_db[simulation_name].drop()

    @staticmethod
    def _prepare_document(document, read_mode=True):
        if read_mode:
            prepared_document = {key: pickle.loads(value) for key, value in document.items()
                                 if key not in ['step', '_id']}
        else:
            prepared_document = {key: pickle.dumps(value) for key, value in document.items() if key != 'step'}
        prepared_document['step'] = document['step']
        return prepared_document

    @staticmethod
    def _delete_id(document):
        prepared_document = {key: value for key, value in document.items() if key not in ['_id']}
        return prepared_document