# Data Transformation Module

This Python project is designed for performing various data transformation tasks on Spark DataFrames. It allows for flexible configuration through JSON, making it easy to apply multiple transformations such as date format standardization, boolean value standardization, language code conversion, handling null values, and more.

## Features

- **Boolean Values Standardization**: Standardizes boolean values across the dataset.
- **Date Format Standardization**: Automatically detects and converts date formats to a specified format.
- **Handling Null Values**: Replaces null values with predefined placeholders.
- **Data Type Standardization**: Converts data types according to the specified format.
- **Currency and Amount Standardization**: Cleans and standardizes currency and amount fields.
- **Language Code Transformations**: Converts language codes to ISO standards.
- **Handling Cross-System IDs**: Manages and reconciles IDs across different systems.
- **Remove Padded Zeros**: Removes leading zeros from numerical strings while optionally preserving the original data.

## Getting Started

### Prerequisites

- Python 3.x
- Apache Spark 3.x
- PySpark

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/dataFrame-core-transformation-module.git
   cd dataFrame-core-transformation-module
   
2. Install the required Python packages:
   pip install pyspark
3. Usage
   3.1 Define your transformations in a JSON configuration file:
     
   ```json
   {
      "transformations": [
        {
          "name": "Boolean Values Standardization",
          "columns": ["isExcludedFromRealign", "isDeleted"],
          "options": {
            "true_values": ["1", "Y", "Yes"],
            "false_values": ["0", "N", "X"]
          }
        },
        {
          "name": "Date Format Standardization",
          "columns": ["expected_activity_date", "last_activity_date"],
          "options": {
            "format": "yyyy-MM-dd"
          }
        },
        {
          "name": "Remove Padded Zeros",
          "columns": ["customer_id"],
          "options": {
            "override_column": false
          }
        },         
       {
         "name": "Language Code Transformations",
         "columns": ["billing_lang__c"],
         "options": {
           "codes_to_iso": {        
             "E": "EN",
             "J": "JP"
           }
         }
       },
       {
         "name": "Handling Cross-System IDs",
         "columns": ["sap_id__c"],
         "options": {
           "primary_system": "SAP",
           "secondary_system": "Salesforce",
           "sap_id_column": "sap_id__c",
           "salesforce_id_column": "id"
         }
       }
      ]
    }
   ```
3.2 Initialize the transformation module and apply it to a DataFrame:
   ```python
    from pyspark.sql import SparkSession
    from data_transformation_module import DataTransformationModule
    
    spark = SparkSession.builder.appName("DataTransformationExample").getOrCreate()
    input_df = spark.table("your_table_name")
    
    json_config = '''(your JSON config here)'''
    transformer = DataTransformationModule(json_config)
    transformed_df = transformer.apply_transformations(input_df)
    
    transformed_df.show()
  ```
