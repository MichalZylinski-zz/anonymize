#!/usr/bin/python

import argparse, sys
from anonymize import Engine, SchemaParser

def main():
    argparser = argparse.ArgumentParser(description="Anonymize your data. Fast.")
    argparser.add_argument("-s", "--schema", help="schema file location", required=True)
    group = argparser.add_mutually_exclusive_group()
    group.add_argument("-e", "--export-storage", help="saves internal mapping dictionary as JSON file")
    group.add_argument("-d", "--dump-storage", action="store_true", help="displays internal mapping dictionary")
    args = argparser.parse_args()
    try:
        if args.schema and args.dump_storage:
            schema = SchemaParser(args.schema)
            print schema.Storage.dump()
        elif args.schema and args.export_storage:
            schema = SchemaParser(args.schema)
            with open(args.export_storage, "w") as f:
                f.write(schema.Storage.dump())
        elif args.schema and args.dump_storage is False and args.export_storage is None:
            schema = SchemaParser(args.schema)
            engine = Engine(input=schema.Input, output=schema.Output, storage=schema.Storage)
            engine.read_rules(schema.Rules)
            engine.run()
    except Exception:
       print sys.exc_info()[1]

if __name__ == "__main__":
    main()