# Why ?
This stack is a spark/scala classification/anonymization as a code project that is here to help us anonymize data using spark

# Features
```json
{
  "db": "db1",
  "tables": [
    {
      "table": "tb1",
      "limit": -1,
      "columns": [
        {
          "column": "col1",
          "classification": "personnelle",
          "policy": "fake_firstname"
        },
        {
          "column": "col2",
          "classification": "sensible",
          "policy": "round_ten"
        },
        {
          "column": "col3",
          "classification": "sensible",
          "policy": "weaken_month"
        }
      ]
    }
  ]
}
```
A policy file is used to list databases and its tables. Within each table, anonymization policy is specified with a classification level. Uncited columns are kept as is.
