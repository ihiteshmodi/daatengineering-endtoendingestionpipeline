import pandas as pd
import uuid

class ETLFunction:
    def __init__(self, template, raw_data):
        self.template = template
        self.raw_data = raw_data
        self.metadata_df :pd.DataFrame = None
        
        # Extracting columns
        try:
            non_null_columns = template.loc[template["Nullable"] == False, "Input Column"]
            date_columns = template.loc[template["Data Type"] == "DATE", ["Input Column", "Current Cast Type"]]
            numeric_columns = template.loc[template["Data Type"] == "NUMBER", "Input Column"]
            required_columns_mask = ~template["Output Column"].isna()
            required_columns_list = template.loc[required_columns_mask, ["Output Column", "Input Column"]]
            rename_columns_mask = ~required_columns_list["Input Column"].isna()
            rename_columns_list = required_columns_list.loc[rename_columns_mask]

            self.non_null_columns = non_null_columns
            self.date_columns = date_columns
            self.numeric_columns = numeric_columns
            self.columns_to_keep = list(required_columns_list["Output Column"])
            self.rename_columns_list = rename_columns_list
            
        except Exception as e:
            raise ValueError(f"An error occurred during initialization: {str(e)}")

    def convert_to_numeric(self):
        try:
            for numeric_column in self.numeric_columns:
                self.raw_data[numeric_column] = pd.to_numeric(self.raw_data[numeric_column], errors='coerce')
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during numeric conversion: {str(e)}")
    
    def drop_null_columns(self):
        try:
            self.raw_data.dropna(subset=self.non_null_columns, inplace=True)
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during dropping null columns: {str(e)}")
    
    def convert_date_columns(self):
        try:
            for index, row in self.date_columns.iterrows():
                column_name = row['Input Column']
                cast_type = row['Current Cast Type']

                self.raw_data[column_name] = pd.to_datetime(self.raw_data[column_name]).dt.strftime(cast_type)
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during date column conversion: {str(e)}")
        
    
    def create_fileid(self):
        try:
            self.random_uuid = uuid.uuid4()
            self.raw_data["FILE_ID"] = str(self.random_uuid)
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred while creating unique file id: {str(e)}")
    
    def rename_columns(self):
        try:
            for index, row in self.rename_columns_list.iterrows():
                new_col_name = row['Output Column']
                old_col_name = row['Input Column']
                self.raw_data.rename(columns={old_col_name: new_col_name}, inplace=True)
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during column renaming: {str(e)}")
    
    def create_hash_key(self):
        try:
            self.raw_data["HASH_KEY"] = self.raw_data["DATE_OF_SALE"].astype(str) + self.raw_data["POSTAL_CODE"].astype(str) + self.raw_data["PROPERTY_TYPE"].astype(str)
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during hash key creation: {str(e)}")
        
    def group_by(self):
        try:
            self.raw_data = self.raw_data.groupby(by = "HASH_KEY").sum().reset_index()
            return self
        except Exception as e:
            raise ValueError(f"An error occurred during groupby: {str(e)}")

    
    def keep_required_columns(self):
        try:
            self.raw_data = self.raw_data[self.columns_to_keep]
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred during keeping required columns: {str(e)}")
        
    def update_metadata(self):
        try:
            data = {
                "FILE_ID": str(self.raw_data["FILE_ID"].unique()),
                "COUNTS_DURING_STAGING": self.raw_data.shape[0],
                "COUNTS_AFTER_INGESTION": None,
            }
            self.metadata_df = pd.DataFrame(data, index = [0])
            return self
        
        except Exception as e:
            raise ValueError(f"An error occurred While creating metadata df: {str(e)}")