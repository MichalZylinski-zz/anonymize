import uuid
import cPickle as pickle
import os, json
from io import *

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
        try: 
            self._json_ = json.load(open(json_file))
        except:
            raise IOError("'%s' schema file invalid." % json_file)
        self.Input = self.__parse_input__()
        self.Output = self.__parse_output__()
        self.Storage = self.__parse_storage__()
        self.Rules = self.__parse_rules__()

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
        input_types = ["file"] #all supported input types
        input_formats = ["csv", "json"]
        type = self._json_["input"].get("inputType")
        properties = self._json_["input"].get("inputProperties")
        if type.lower() not in input_types:
            raise ValueError("inputType: %s is not supported" % type)
        elif type.lower() == "file": #preparing FileReader object
            file_name = properties.get("fileName")
            format = properties.get("format")
            schema = properties.get("schema")
            separator = properties.get("separator") or ","
            quote = properties.get("quote") or ""
            if format.lower() not in input_formats:
                raise ValueError("Input format: %s is not supported" % format)
            elif format.lower() == "csv":
                format = CSVFormat(sep=separator, quote=quote, schema=schema)
            elif format.lower() == "json":
                format = JSONFormat()
            return FileReader(file_name, format)

    def __parse_output__(self):
        output_types = ["file"]
        output_formats = ["csv", "json"]
        type = self._json_["output"].get("outputType")
        properties = self._json_["output"].get("outputProperties")
        if type.lower() not in output_types:
            raise ValueError("outputType: %s is not supported" % type)
        elif type.lower() == "file":
            file_name = properties.get("fileName")
            format = properties.get("format")
            separator = properties.get("separator") or ","
            quote = properties.get("quote") or ""
            replace = properties.get("replace") or False
            if format.lower() not in output_formats:
                raise ValueError("Output format: %s is not supported" % format)
            elif format.lower() == "csv":
                format = CSVFormat(sep=separator, quote=quote, schema=schema)
            elif format.lower() == "json":
                format = JSONFormat()
            return FileWriter(file_name, format, replace)

class Engine:
    def __init__(self, input, output, storage):
        self.__input__ = input
        self.__output__ = output
        self.__storage__ = storage
        self.__rules__ = []

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
            if r.Type == "replace":
                object[r.FieldName] = self.__storage__.replace(r.GlobalName, object[r.FieldName])
        return object

    def run(self):
        with self.__input__ as f:
            for line in f:
                o = self.apply_rules(line)
                self.__output__.write(o)
        self.__output__.close()
        self.__storage__.flush()

    def save(self):
        """
        Saves replacement values in permanent storage.
        """
        self.__storage__.flush()

class MemoryStorage:
    def __init__(self, file, restore=True):
        self.__f__ = file
        if os.path.isfile(self.__f__):
            self.__storage__ = pickle.load(open(self.__f__, "r"))
        else:
            self.__storage__ = {}

    def replace(self, name, value):
        if self.__storage__.has_key(name):
            storagens = self.__storage__.get(name) #setting dictionary namespace
        else:
            storagens = self.__storage__[name] = {} #creating new dictionary namespace
        if storagens.has_key(value):
            return storagens.get(value)
        else:
            hash = self.generate_hash()
            storagens[value] = hash
            return hash

    def generate_hash(self):
            return uuid.uuid4().get_hex()

    def dump(self):
        """
        Returns internal mapping dictionary as JSON-formatted string.
        """
        return json.dumps(self.__storage__)

    def flush(self):
        """
        Saves current state of storage to disk.
        """
        pickle.dump(self.__storage__, open(self.__f__, "w"))
