# ReactiveDB

TLDR: A database that will allow developers to transform their data in database. You will then be able to query and listen for changes to your data.

This is very early in development and I would not use this for anything serious.

## Concepts
This database centers on the idea that instead of computing changes to data as you need it, you should compute changes as you recieve it.

### Source Table
A table that you can directly insert raw data into. Defined in the config file like below:
```yaml
- Source:
    name: testTable
    columns:
      sampleColumn1: Integer
      sampleColumn2: String
      sampleColumn3: Bool
```

### Derived Table
A table defined in the configuration file which is either a Function, Filter, or Union. This takes in either a source table or another derived table and generates a new table based on the configuration. Defined in the config file like below:
```yaml
- Derived:
    name: derivedTable
    transform_definition: 
      (Transform defintion here)
```

### Function, Filter, or Union
The basic data operations allowed in ReactiveDB

- Function (Change one or more columns from an input table into another):
```yaml
FunctionTransform:
  source_table: inputTable
  functions:
    - newColumn ~ inputTableColumn + 2
```
- Filter (Only write entries that pass a given conditon to the derived table):
```yaml
FilterTransform:
  source_table: inputTable
  filter: (inputTableColumn > 11) && (inputTableColumn < 14)
```
- Union (Combine entries from two different tables based on a column value)
```yaml
UnionTransform:
  tables_and_foreign_keys:
  - - table1
    - column1
  - - table2
    - column2
 ```

## Getting started
To run this very early version go into reactive_db/ and run `cargo run 1108 test_cfg.yaml` then use either the python client or the rust client (Samples are under `examples/sample.py` and `src/bin/usage_example.rs` respectively) to interact with the database. 
