### Declarative schema in a YAML file format

The schema provided there: [schema](src/schema.yaml). The yaml-validator is use to check the schema validity.

Key features: 

> [!TIP]
> - Track schema changes in a single file with github history
> - Append DB structure with generated SQL safely, do not perform destructive SQL 
> - Aggregate changes from multiple commiters and tolerate to execution order 

![yaml](yaml1.png "title")
