#!/usr/bin/python

import argparse, sys
from anonymize import Engine, SchemaParser
import logging
import signal

global engine


def main():
    argparser = argparse.ArgumentParser(description="Anonymize your data. Fast.")
    argparser.add_argument("-s", "--schema", help="schema file location", required=True)
    argparser.add_argument("-v", "--verbose", help="display progress information", action="store_true")
    group = argparser.add_mutually_exclusive_group()
    group.add_argument("-e", "--export-storage", help="saves internal mapping dictionary as JSON file")
    group.add_argument("-d", "--dump-storage", action="store_true", help="displays internal mapping dictionary")
    args = argparser.parse_args()
    if args.schema and args.dump_storage:
        schema = SchemaParser(args.schema)
        print(schema.Storage.dump())
    elif args.schema and args.export_storage:
        schema = SchemaParser(args.schema)
        with open(args.export_storage, "w") as f:
            f.write(schema.Storage.dump())
    elif args.schema and args.dump_storage is False and args.export_storage is None:
        if args.verbose:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", filename="anonymize.log", filemode="w")
        else:
            logging.basicConfig(level=logging.ERROR, format="%(asctime)s %(message)s", filename="anonymize.log", filemode="w")
        schema = SchemaParser(args.schema)
        engine = Engine(input=schema.Input, output=schema.Output, storage=schema.Storage)
        engine.read_rules(schema.Rules)
        try:
            if schema.Input.Type == "stream":
                print("Processing started in streaming mode... [Press Ctrl+C to abort]")
            engine.run()
            if args.verbose:
                print(("Processed %s in %.2f sec" % (engine.ProcessedRows, engine.ProcessingTime.total_seconds())))
                print(("[Avg %.0f rows/sec]" % (engine.ProcessedRows/(engine.ProcessingTime.total_seconds() or 0.01))))
        except KeyboardInterrupt:
            engine.save()
            print("Processing interrupted.")
        
if __name__ == "__main__":
    main()