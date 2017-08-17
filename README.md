# anonymize

Anonymize is fast and easy-to-use toolkit for massive data masking and removal. Written in Python, it offers both end user (command line) interface and programming API.

## Installation

Anonymize is available through PyPI and requires Python to work (it has been tested with both Python 2.7 and Python 3.6). Once you have Python running,  installation takes only one command and a few seconds to complete:

```
pip install anonymize
```

## Command line usage

The most basic and common usage scenario is relying on command-line interface. After succesful installation your operating system should be equipped with new command: `anonymize`. Try typing `anonymize -h` to see the list of all available parameters:

```
usage: anonymize [-h] -s SCHEMA [-v] [-e EXPORT_STORAGE | -d]

Anonymize your data. Fast.

optional arguments:
  -h, --help            show this help message and exit
  -s SCHEMA, --schema SCHEMA
                        schema file location
  -v, --verbose         display progress information
  -e EXPORT_STORAGE, --export-storage EXPORT_STORAGE
                        saves internal mapping dictionary as JSON file
  -d, --dump-storage    displays internal mapping dictionary
```
Anonymize command always requires JSON-based configuration file called schema. It contains all necessary settings, like input and output definitions and transformation rules. [Here's basic example](https://raw.githubusercontent.com/MichalZylinski/anonymize/master/examples/weblog/weblog_schema.json) of schema you may start with. There's also [detailed documentation](/docs/schema_reference.md) provided as well.

### Running data masking job

As all parameters are stored in configuration file, running the job requires only one parameter to run `-s`. In following example `--verbose` switch has been used as well to get more information about the results:
```
anonymize -s weblog_schema.json -v

Processed 1000 in 0.03 sec
[Avg 29731 rows/sec]
```

### Exporting metainformation
Sometimes it is useful to get the information about actual mappings between original data and hashes. It is all stored in [metarepository](/docs/architecture.md) and can be easily extracted to JSON format using either `-d` or `-e` parameters. The example below exports the repository to file called `repo.json`:

```
anonymize -s weblog_schema.json -e repo.json
```


## More documentation

* [JSON schema documentation](/docs/schema_reference.md)
* [Command-line reference](/docs/cli_reference.md)
* [Internal architecture and programming API](/docs/architecture.md)
* [Examples](/examples/)

