import uuid
import pickle as pickle
import os, json
import pika
from .io import *
import logging
from datetime import datetime

class EngineRule:
    def __init__(self, type, field_name, global_name=None):
        self.Type = type
        if self.Type not in ["replace","remove"]:
            raise ValueError("%s rule type not supported." % type)
        self.FieldName = field_name
        if self.Type == "replace" and global_name is None:
            raise ValueError("global_name parameter is required for 'replace' type rules")
        self.GlobalName = global_name


class SchemaParser:
    def __init__(self, json_file):
        logging.info("Schema parsing started: %s" % json_file) 
        try: 
            self._json_ = json.load(open(json_file))
        except:
            raise IOError("'%s' schema file invalid." % json_file)
        self.Input = self.__parse_input__()
        self.Rules = self.__parse_rules__()
        self.Output = self.__parse_output__()
        self.Storage = self.__parse_storage__()
        logging.info("Schema parsing completed")


    def __parse_rules__(self):
        rules = []
        if self._json_["rules"].get("remove"):
            for remove_rule in self._json_["rules"].get("remove"):
                rules.append(EngineRule("remove", remove_rule.get("fieldName")))
        if self._json_["rules"].get("replace"):
            for replace_rule in self._json_["rules"].get("replace"):
                rules.append(EngineRule("replace", replace_rule.get("fieldName"), replace_rule.get("globalName")))
        return rules

    def __parse_storage__(self):
        storage_types = ["memory"]
        type = self._json_["storage"].get("storageType")
        properties = self._json_["storage"].get("storageProperties")
        if type.lower() not in storage_types:
            raise ValueError("storageType: %s is not supported" % type)
        elif type.lower() == "memory":
            file_name = properties.get("fileName")
            restore = properties.get("restore") or True
            return MemoryStorage(file_name, restore)

    def __parse_input__(self):
        input_types = ["file", "rabbitmq"] #all supported input types
        input_formats = ["csv", "json"]
        type = self._json_["input"].get("inputType")
        properties = self._json_["input"].get("inputProperties")
        separator = properties.get("separator") or ","
        quote = properties.get("quote") or ""
        format = properties.get("format")
        schema = properties.get("schema")
        self.Schema = schema
        if format.lower() not in input_formats:
            raise ValueError("Input format: %s is not supported" % format)
        elif format.lower() == "csv":
            format = CSVFormat(sep=separator, quote=quote, schema=schema)
        elif format.lower() == "json":
            format = JSONFormat()
        if type.lower() not in input_types:
            raise ValueError("inputType: %s is not supported" % type)
        elif type.lower() == "file": #preparing FileReader object
            file_name = properties.get("fileName")
            return FileReader(file_name, format)
        elif type.lower() == "rabbitmq":
            connection_string = properties.get("URI")
            queue_name = properties.get("QueueName")
            return RabbitMQReader(connection_string, queue_name, format)

    def __parse_output__(self):
        output_types = ["file", "rabbitmq"]
        output_formats = ["csv", "json"]
        type = self._json_["output"].get("outputType")
        properties = self._json_["output"].get("outputProperties")
        separator = properties.get("separator") or ","
        quote = properties.get("quote") or ""
        format = properties.get("format")
        if format.lower() not in output_formats:
            raise ValueError("Output format: %s is not supported" % format)
        elif format.lower() == "csv":
            schema = list(self.Input.Format.schema) #making a copy of input schema
            for r in [r.FieldName for r in self.Rules if r.Type == "remove"]:
                schema.remove(r)
            format = CSVFormat(sep=separator, quote=quote, schema=schema)
        elif format.lower() == "json":
            format = JSONFormat()
        if type.lower() not in output_types:
            raise ValueError("outputType: %s is not supported" % type)
        elif type.lower() == "file":
            file_name = properties.get("fileName")
            replace = properties.get("replace") or False
            return FileWriter(file_name, format, replace)
        elif type.lower() == "rabbitmq":
            connection_string = properties.get("URI")
            queue_name = properties.get("QueueName")
            return RabbitMQWriter(connection_string, queue_name, format)

class Engine:
    def __init__(self, input, output, storage):
        self.__input__ = input
        self.__output__ = output
        self.__storage__ = storage
        self.__rules__ = []
        self.ProcessedRows = 0
        self.ProcessingTime = 0

    def read_rules(self, rules):
        """
        Reads list of EngineRule objects. Usually rules are property of SchemaParser.
        """
        self.__rules__ = rules

    def add_rule(self, rule):
        """
        Adds EngineRule object to rules list.
        """
        self.__rules__.append(rule)

    def apply_rules(self, object):
        for r in self.__rules__:
            if r.Type == "remove":
                object.pop(r.FieldName)
            if r.Type == "replace" and object[r.FieldName] is not "": 
                object[r.FieldName] = self.__storage__.replace(r.GlobalName, object[r.FieldName])
        return object

    def run(self):
        logging.info("Processing engine started")
        if self.__input__.Type == "file":
            start = datetime.now()
            rows = 0
            with self.__input__ as f:
                for line in f:
                    o = self.apply_rules(line)
                    self.__output__.write(o)
                    rows += 1
                    if (rows % 100000) == 0: logging.info("Processed %s rows" % rows)
            self.__output__.close()
            self.__storage__.flush()
            self.ProcessedRows = rows
            self.ProcessingTime = datetime.now() - start
            logging.info("Processing engine finished")            

        elif self.__input__.Type == "stream":
            self.__input__.run(self)

    def save(self):
        """
        Saves replacement values in permanent storage.
        """
        self.__storage__.flush()

class MemoryStorage:
    def __init__(self, file, restore=True):
        self.__f__ = file
        if os.path.isfile(self.__f__):
            with open(self.__f__, "rb") as f:
                self.__storage__ = pickle.load(f)
        else:
            self.__storage__ = {}

    def replace(self, name, value):
        if name in self.__storage__:
            storagens = self.__storage__.get(name) #setting dictionary namespace
        else:
            storagens = self.__storage__[name] = {} #creating new dictionary namespace
        if value in storagens:
            return storagens.get(value)
        else:
            hash = self.generate_hash()
            storagens[value] = hash
            return hash

    def generate_hash(self):
            return uuid.uuid4().hex

    def dump(self):
        """
        Returns internal mapping dictionary as JSON-formatted string.
        """
        return json.dumps(self.__storage__)

    def flush(self):
        """
        Saves current state of storage to disk.
        """
        logging.info("Storage flushing started...")
        with open(self.__f__, "wb") as f:
            pickle.dump(self.__storage__, f)
        logging.info("Flushing completed succefully.")