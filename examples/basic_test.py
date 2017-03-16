#!/usr/bin/python

from anonymize.io import FileReader, FileWriter, ConsoleWriter, CSVFormat, JSONFormat
from anonymize import Engine, MemoryStorage, EngineRule

storage = MemoryStorage("storage.bin", restore=True)

#
# Test 1 - Schemaless example
#

input = FileReader("test.csv", CSVFormat())
output = FileWriter("test_output1.json", JSONFormat(), replace=True)

e = Engine(input,output,storage)
e.add_rule(EngineRule("remove","0")) #removing first/date column
e.add_rule(EngineRule("replace", "1", "id")) #replacing second/id column with hashed value
e.run()

#
#Test 2 - Using explicit schema
#

schema = ["Date","SessionId","Value"]
input2 = FileReader("test.csv", CSVFormat(schema=schema))
output2 = FileWriter("test_output2.json", JSONFormat(), replace=True)
e = Engine(input2, output2, storage)
e.add_rule(EngineRule("remove", "Date"))
e.add_rule(EngineRule("replace", "SessionId", global_name="id"))
e.run()

#
#Test 3 - Reading JSON data and saving it back as CSV
#

input3 = FileReader("test_output1.json", JSONFormat())
output3 = ConsoleWriter(CSVFormat())
e = Engine(input3, output3, storage)
e.add_rule(EngineRule("replace", "1", global_name="id"))
e.run()

