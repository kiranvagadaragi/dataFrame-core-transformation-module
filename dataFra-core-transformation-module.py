import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, IntegerType, BooleanType

class DataTransformationModule:

    def __init__(self, json_config):
        self.config = json.loads(json_config)
        self.transformations = self._normalize_transformations(self.config["transformations"])

    def _normalize_transformations(self, transformations):
        """Normalize column names to lowercase for case-insensitive processing."""
        normalized_transformations = []
        for transformation in transformations:
            norm_transformation = transformation.copy()
            norm_transformation["columns"] = [col.lower() for col in transformation["columns"]]
            options = transformation.get("options", {})
            if "codes_to_iso" in options:
                options["codes_to_iso"] = {k.lower(): v for k, v in options["codes_to_iso"].items()}
            norm_transformation["options"] = options
            normalized_transformations.append(norm_transformation)
        return normalized_transformations

    def apply_transformations(self, df):
        # Convert DataFrame column names to lowercase
        df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

        for transformation in self.transformations:
            name = transformation["name"]
            columns = transformation["columns"]
            options = transformation.get("options", {})

            if name == "Boolean Values Standardization":
                df = self._boolean_values_standardization(df, columns, options)

            elif name == "Date Format Standardization":
                df = self._date_format_standardization(df, columns, options)

            elif name == "Handling Null Values":
                df = self._handle_null_values(df, columns, options)

            elif name == "Data Type Standardization":
                df = self._data_type_standardization(df, columns, options)

            elif name == "Currency and Amount Standardization":
                df = self._currency_amount_standardization(df, columns, options)

            # elif name == "Hierarchical Data Flattening":
            #     df = self._hierarchical_data_flattening(df, columns, options)

            elif name == "Language Code Transformations":
                df = self._language_code_transformation(df, columns, options)

            elif name == "Handling Cross-System IDs":
                df = self._handle_cross_system_ids(df, columns, options)
            
            elif name == "Remove Padded Zeros":
                df = self._remove_padded_zeros(df, columns, options)

        return df

    def _boolean_values_standardization(self, df, columns, options):
        true_values = options.get("true_values", ["1", "y", "yes"])
        false_values = options.get("false_values", ["0", "n", "x"])

        for col in columns:
            df = df.withColumn(col, F.col(col).cast(StringType()))
            df = df.withColumn(
                col,
                F.when(F.col(col).isin(true_values), F.lit(True))
                .when(F.col(col).isin(false_values), F.lit(False))
                .otherwise(F.lit(None).cast(BooleanType()))  # Handle malformed data by setting to None
            )
        
        return df

    def _date_format_standardization(self, df, columns, options):
        output_format = options.get("format", "yyyy-MM-dd")

        # List of potential input date formats to try
        # List of potential input date formats to try
        input_formats = [
            "MM/dd/yyyy HH:mm:ss.SSSSSS",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy",
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "dd-MM-yyyy HH:mm:ss.SSSSSS",
            "dd-MM-yyyy HH:mm:ss",
            "dd-MM-yyyy",
            "yyyy/MM/dd HH:mm:ss.SSSSSS",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy/MM/dd",
            "dd/MM/yyyy HH:mm:ss.SSSSSS",
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy",
            "yyyy.MM.dd HH:mm:ss.SSSSSS",
            "yyyy.MM.dd HH:mm:ss",
            "yyyy.MM.dd",
            "dd.MM.yyyy HH:mm:ss.SSSSSS",
            "dd.MM.yyyy HH:mm:ss",
            "dd.MM.yyyy",
            "yyyyMMdd",
            "ddMMyyyy",
            "MMddyyyy",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy/MM/dd'T'HH:mm:ss.SSSSSS",
            "yyyy/MM/dd'T'HH:mm:ss",
            "yyyy/MM/dd'T'HH:mm:ss'Z'",
            "yyyy.MM.dd'T'HH:mm:ss.SSSSSS",
            "yyyy.MM.dd'T'HH:mm:ss",
            "yyyy.MM.dd'T'HH:mm:ss'Z'"
            # Add more formats as needed
        ]

        for col in columns:
            # Attempt to parse with different formats until one works
            for fmt in input_formats:
                df = df.withColumn(
                    col,
                    F.when(
                        F.to_date(F.col(col), fmt).isNotNull(),  # Check if the format is correct
                        F.date_format(F.to_date(F.col(col), fmt), output_format)
                    ).otherwise(F.col(col))  # Leave it unchanged if it doesn't match
                )
            
            # Set invalid values (still not matching) to null
            df = df.withColumn(
                col,
                F.when(
                    F.to_date(F.col(col), output_format).isNull(),
                    F.lit(None)
                ).otherwise(F.col(col))
            )

        return df


    def _handle_null_values(self, df, columns, options):
        numeric_placeholder = options.get("numeric_null_placeholder", 0)
        string_placeholder = options.get("string_null_placeholder", "")
        
        for col in columns:
            col_type = df.schema[col].dataType
            if isinstance(col_type, StringType):
                df = df.withColumn(
                    col, 
                    F.when(F.col(col).isNull(), F.lit(string_placeholder)).otherwise(F.col(col))
                )
            else:
                df = df.withColumn(
                    col, 
                    F.when(F.col(col).isNull(), F.lit(numeric_placeholder)).otherwise(F.col(col))
                )
        
        return df

    def _data_type_standardization(self, df, columns, options):
        types = options.get("types", {})
        
        for col in columns:
            desired_type = types.get(col)
            if desired_type == "float":
                df = df.withColumn(col, F.col(col).cast(FloatType()))
            elif desired_type == "int":
                df = df.withColumn(col, F.col(col).cast(IntegerType()))
            # Add more type transformations as needed
            
        return df

    def _currency_amount_standardization(self, df, columns, options):
        currency_code_column = options.get("currency_code_column", "currency_code")

        for col in columns:
            df = df.withColumn(
                col,
                F.when(
                    F.col(col).rlike(r'^[0-9.,]+$'),  # Only keep numbers and periods, commas as thousand separators
                    F.regexp_replace(F.col(col), r'[^0-9.]', '').cast(FloatType())
                ).otherwise(F.lit(None))  # Set invalid values to null
            )
        
        return df

    def _language_code_transformation(self, df, columns, options):
        codes_to_iso = options.get("codes_to_iso", {})

        for col in columns:
            if col in df.columns:
                # Build the CASE WHEN SQL expression
                cases = " ".join([f"WHEN '{k}' THEN '{v}'" for k, v in codes_to_iso.items()])
                case_expr = f"CASE {col} {cases} ELSE {col} END"
                
                # Apply the transformation
                df = df.withColumn(col, F.expr(case_expr))

        return df

    def _handle_cross_system_ids(self, df, columns, options):
        primary_system = options.get("primary_system", "SAP")
        secondary_system = options.get("secondary_system", "Salesforce")
        sap_id_column = options.get("sap_id_column", "sap_customer_id")
        salesforce_id_column = options.get("salesforce_id_column", "sf_account_id")

        for col in columns:
            if sap_id_column in df.columns and salesforce_id_column in df.columns:
                df = df.withColumn(
                    col,
                    F.when(F.col(sap_id_column).isNotNull(), F.col(sap_id_column))
                    .otherwise(F.col(salesforce_id_column))
                )    
        
        return df
    
    def _remove_padded_zeros(self, df, columns, options):
        override_column = options.get("override_column", False)
        print(override_column)        
        for col in columns:
            transformed_col = F.regexp_replace(F.col(col).cast(StringType()), r'^0+(?!$)', '')
            if override_column:
                df = df.withColumn(col, transformed_col)
            else:
                new_col_name = f"{col}_shortcode"
                df = df.withColumn(new_col_name, transformed_col)
        
        return df
    
    
