import os, json, pika, sys

class GenericReader:
    def __iter__(self):
        return self

    def __next__(self):
        return self.read()

    def next(self):
        return self.__next__()

    def __enter__(self):
        return self

class FileReader(GenericReader):
    def __init__(self, file_name, format):
        self.Type = "file"
        if not os.path.exists(file_name):
            raise IOError("File '%s' does not exist." % file_name)
        self.__f__= open(file_name, mode="r")
        self.Format = format

    
    def read(self):
        """
        Returns internal representation (dictionary) of single line
        """
        line = self.__f__.readline()
        if line == "": raise StopIteration
        formatted_line = self.Format.parse(line)
        return formatted_line

    def __exit__(self, type, value, traceback):
        self.__f__.close()

class RabbitMQReader:
    def __init__(self, connection_string, queue_name, format):
        self.Type = 'stream'
        connection = pika.BlockingConnection(pika.URLParameters(connection_string))
        self.__channel__ = connection.channel()
        self.__queue_name__ = queue_name
        self.__engine__ = None
        self.Format = format

    def __get_message__(self, ch, method, properties, body):
        o = self.__engine__.apply_rules(self.Format.parse(body.decode('utf-8'))) #decodes binary message to UTF-8
        self.__engine__.__output__.write(o)
                                                                
    def run(self, engine):
        self.__engine__ = engine
        self.__channel__.basic_consume(self.__get_message__, queue=self.__queue_name__, no_ack=True)
        self.__channel__.start_consuming()


class FileWriter:
    def __init__(self, file_name, format, replace=False):
        if replace is False and os.path.exists(file_name):
            raise IOError("Output file already exists. Use replace=TRUE to override")
        self.__f__ = open(file_name, mode="w")
        self.Format = format

    def write(self, object):
        self.__f__.write(self.Format.format(object)+"\n")

    def close(self):
        self.__f__.close()

class RabbitMQWriter:
    def __init__(self, connection_string, queue_name, format):
        self.Type = "stream"
        self.connection = pika.BlockingConnection(pika.URLParameters(connection_string))
        self.__channel__ = self.connection.channel()
        self.__queue_name__ = queue_name
        self.__channel__.queue_declare(queue=queue_name)
        self.Format = format

    def write(self, object):
        formatted_object = self.Format.format(object)
        self.__channel__.basic_publish(exchange="", routing_key=self.__queue_name__, body=formatted_object)

    def close(self):
        self.connection.close()

class ConsoleWriter:
    def __init__(self, format):
        self.Type = "console"
        self.Format = format

    def write(self, object):
        print((self.Format.format(object)[:-1]))

    def close(self):
        pass

class CSVFormat:
    def __init__(self, sep=",", quote="", schema=None):
        self.separator = sep
        self.quote = quote
        self.schema = schema

    def parse(self, object):
        try:
            from io import StringIO
        except:
            from cStringIO import StringIO
        import csv
        with StringIO(object) as csvline:
            if self.quote:
                reader = csv.reader(csvline, delimiter=self.separator, quotechar=self.quote)
            else:
                reader = csv.reader(csvline, delimiter=self.separator)
            row = reader.__next__()
        #removing EOL character
        if object[-1:] == "\n":
            object = object[:-1]
        if self.schema:
            return dict(list(zip(self.schema, row)))
        else:
            columns = row
            colindex = [str(c) for c in range(len(columns))]
            return dict(list(zip(colindex,columns)))

    def format(self, object):
        values = []
        if self.schema is None:
            values = list(object.values())
        else:
            for o in self.schema:
                values.append(object[o])
        return self.separator.join([self.quote+i+self.quote for i in values])

class JSONFormat:
    """
    Reads and writes JSON structured data.
    """
    def parse(self, object):
        return json.loads(object)

    def format(self, object):
        return json.dumps(object)