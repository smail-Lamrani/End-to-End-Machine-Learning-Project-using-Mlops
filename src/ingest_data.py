# import os
# import zipfile 
# from abc import ABC , abstractmethod
# import pandas as pd 

# # Define an abstract class for  Data Ingestor
# class DataIngestor(ABC):
#     @abstractmethod
#     def ingest(self, file_path: str) -> pd.DataFrame:
#         """Abstract method to ingest data from a given file """
#         pass

# # Concrete implementation for ZIP Ingestion
# class ZipDataIngestor(DataIngestor):
#     def ingest(self, file_path: str) -> pd.DataFrame:
#         """Extracts a.zip file  a returns the context as a """
#         # Ensure the file is a .zip 
#         if not file_path.endswith('.zip'):
#             raise ValueError("Provided file is not a .zip file;")
#         #Extract the zip file 
#         with zipfile.ZipFile(file_path, 'r') as zip_ref:
#             zip_ref.extractall("extracted_data")
#         #Find the extractes CSV file (assuming there is one CSV file)
#         extracted_files= os.listdir("extracted_data")
#         csv_files =[f for f in extracted_files if f.endswith(".csv")]
#         if len(csv_files) == 0 :
#             raise FileNotFoundError("No CSV file found in the exteracted data .")
#         if len(csv_files) > 1 :
#             raise ValueError("Multiple CSV files found.Please specify wich one to use .")
#         #Read the csv into dataframe 
#         csv_file_path = os.path.join("extracted_data",csv_files[0])
#         df= pd.read_csv(csv_file_path)
#         #Return to DataFrame
#         return df 
# # Implement a factory to create DataIngestor :
# class DataIngestorFactory:
#     @staticmethod
#     def get_data_ingestor(file_extension : str) -> DataIngestor:
#         """Returns the appropriate DataIngestor based on file extension """
#         if file_extension == 'zip':
#             return ZipDataIngestor()
#         else:
#             raise ValueError(f"No ingestor available for file extension: {file_extension}")
# # Example usage:
# if __name__ == "__main__":
#     # # Specify the file path
#     file_path = r"C:\Users\Admin\Desktop\End-to-End_Machine_Learning_Project\data\archive.zip"
#     # # Determine the file extension
#     file_extension = os.path.splitext(file_path)[1]

#     # # Get the appropriate DataIngestor
#     data_ingestor = DataIngestorFactory.get_data_ingestor(file_extension)

#     # # Ingest the data and load it into a DataFrame
#     df = data_ingestor.ingest(file_path)

#     # #Now df contains the DataFrame from the extracted CSV
#     print(df.head())  # Display the first few rows of the DataFrame
#     pass
       
import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd


# ============================================================
# Abstract Base Class: DataIngestor
# ============================================================
class DataIngestor(ABC):
    """Abstract base class defining a common interface for data ingestion."""

    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Ingest data from a file and return it as a DataFrame."""
        pass


# ============================================================
# Concrete Implementation: ZipDataIngestor
# ============================================================
class ZipDataIngestor(DataIngestor):
    """Handles ingestion of .zip archives containing CSV files."""

    def ingest(self, file_path: str) -> pd.DataFrame:
        # --- 1. Validate file extension ---
        if not file_path.lower().endswith('.zip'):
            raise ValueError("Provided file is not a .zip file.")

        # --- 2. Extract the ZIP archive ---
        extract_dir = "extracted_data"
        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # --- 3. Find extracted CSV files ---
        extracted_files = os.listdir(extract_dir)
        csv_files = [f for f in extracted_files if f.lower().endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError("No CSV file found in the extracted data.")
        if len(csv_files) > 1:
            raise ValueError(
                f"Multiple CSV files found ({csv_files}). Please specify which one to use."
            )

        # --- 4. Read the single CSV file into a DataFrame ---
        csv_path = os.path.join(extract_dir, csv_files[0])
        df = pd.read_csv(csv_path)

        print(f"[INFO] Loaded '{csv_files[0]}' successfully ({df.shape[0]} rows, {df.shape[1]} cols)")
        return df


# ============================================================
# Factory Class: DataIngestorFactory
# ============================================================
class DataIngestorFactory:
    """Factory that returns the appropriate DataIngestor implementation."""

    @staticmethod
    def get_data_ingestor(file_extension: str) -> DataIngestor:
        """Returns the correct DataIngestor based on the file extension."""
        normalized_ext = file_extension.lower().lstrip('.')

        if normalized_ext == 'zip':
            return ZipDataIngestor()
        else:
            raise ValueError(f"No ingestor available for file extension: {file_extension}")


# ============================================================
# Example Usage
# ============================================================
if __name__ == "__main__":
    # Define file path (use raw string to avoid unicode escape issues)
    file_path = r"C:\Users\Admin\Desktop\End-to-End_Machine_Learning_Project\data\archive.zip"

    # Extract file extension (without leading dot)
    file_extension = os.path.splitext(file_path)[1].lstrip('.')

    # Create the appropriate DataIngestor instance via the factory
    data_ingestor = DataIngestorFactory.get_data_ingestor(file_extension)

    # Ingest data and display preview
    df = data_ingestor.ingest(file_path)
    print(df.head())
 