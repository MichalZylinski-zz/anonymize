import os, json

class FileReader:
    def __init__(self, file_name, format):
        if not os.path.exists(file_name):
            raise IOError("File '%s' does not exist." % file_name)
        self.__f__= open(file_name, mode="r")
        self.__format__ = format

    def __iter__(self):
        return self

    def next(self):
        return self.read()

    def __enter__(self):
        return self
    
    def read(self):
        """
        Returns internal representation (dictionary) of single line
        """
        line = self.__f__.readline()
        #print "a"+line+"b"
        if line == "": raise StopIteration
        formatted_line = self.__format__.parse(line)
        return formatted_line

    def __exit__(self, type, value, traceback):
        self.__f__.close()

class FileWriter:
    def __init__(self, file_name, format, replace=False):
        if replace is False and os.path.exists(file_name):
            raise IOError("Output file already exists. Use replace=TRUE to override")
        self.__f__ = open(file_name, mode="w")
        self.__format__ = format

    def write(self, object):
        self.__f__.write(self.__format__.format(object)+"\n")

    def close(self):
        self.__f__.close()

class ConsoleWriter:
    def __init__(self, format):
        self.__format__ = format

    def write(self, object):
        print self.__format__.format(object)

    def close(self):
        pass

class CSVFormat:
    def __init__(self, sep=",", quote="", schema=None):
        self.__sep__ = sep
        self.__quote__ = quote
        self.__schema__ = schema

    def parse(self, object):
        #removing EOL character
        object = object[:-1]
        if self.__schema__:
            return dict(zip(self.__schema__, object.split(self.__sep__)))
        else:
            columns = object.split(self.__sep__)
            colindex = [str(c) for c in range(len(columns))]
            return dict(zip(colindex,columns))

    def format(self, object):
        return self.__sep__.join([self.__quote__+i+self.__quote__ for i in object.values()])

class JSONFormat:
    """
    Reads and writes JSON structured data.
    """
    def parse(self, object):
        return json.loads(object)

    def format(self, object):
        return json.dumps(object)