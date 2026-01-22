# Data Eng instructions
Project represents implementation of requirements stated in the file
Data_Eng_Instructions.pdf (See current root folder)

Source data located in the "resources" folder of current project.

# Setup instructions
Install and set java version 17
java -version - be sure spark is working above jvm (scala native)

Install pyspark:
pip install pyspark

Install necessary libraries:
```
    sdk install java 17.0.11.fx-zulu -> for example
    pip install "pyarrow>=15.0.0"
    pip install "grpcio>=1.48.1"
    pip install "grpcio-status >= 1.48.1"
    pip install "zstandard >= 0.25.0"
    Test dependencies:
    pip install nose
    pip install pytest
```

# Use-cases from document "Data_Eng_Instructions.pdf":
Task 2: queries represented in the directory 'tests/integration/sql' under the appropriate names. <br>
Task 3: 2. 'tests/unit/transform/manufactoring_factory_transform_test/test_filter_out_invalid_defects_definitions' <br>
Task 3. Scope 3-4: <br>
- data warehouse design represented in the file 'dwh_model_v3.jpg'
- implementation was done for the fact 'manufacturing_factory_dataset' except dimensions downtime reason and defect. 
- see 'data_end_instructions/main' class as implementation
- see 'tests/*' directory containing necessary tests
- see result data in directory "storage"
- in order to check exported fact table use 'tests/integration/reader/manufactory_factory_fact_reader_test.py'