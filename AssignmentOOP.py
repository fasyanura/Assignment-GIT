# Task 1: Basic OOP Buatlah class MarketingDataETL yang memiliki tiga metode:
#extract(): akan membaca data dari sebuah file CSV (Misalkan, marketing_data.csv)
import pandas as pd

# Define the URL of the CSV file
url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(url, delimiter=';')

# Display the DataFrame
print(df)

#transform(): akan melakukan pembersihan dan transformasi sederhana pada data (seperti mengubah format tanggal atau membersihkan nilai yang kosong)
df.dropna(inplace=True)
df['purchase_date'] = pd.to_datetime(df['purchase_date'], format='%d/%m/%y')
df['purchase_date'] = df['purchase_date'].dt.strftime('%Y-%m-%d')
print(df)

#store(): akan menyimpan data yang telah ditransformasi ke dalam struktur data DataFrame
output_file = "OPP_update.csv"
df.to_csv(output_file, index=False)
print("Data has been successfully saved to file name as", output_file)

df_new = pd.read_csv("OPP_update.csv")
print(df_new)

import pandas as pd

class MarketingDataETL:
    def __init__(self, url):
        self.url = url
        self.data = None

    def extract(self):
        # Read the CSV file into a pandas DataFrame
        self.data = pd.read_csv(self.url, delimiter=';')
        print("extract Done")

    def transform(self):
        # Drop rows with missing values (NA)
        if self.data is not None:
            self.data.dropna(inplace=True)
            # Convert 'purchase_date' column to datetime format (YYYY-MM-DD)
            self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], format='%d/%m/%y')
            # Reformat 'purchase_date' column to YYYY-MM-DD
            self.data['purchase_date'] = self.data['purchase_date'].dt.strftime('%Y-%m-%d')
            print("transform Done")

    def store(self, output_file):
        # Store the transformed DataFrame to a CSV file
        if self.data is not None:
            self.data.to_csv(output_file, index=False)
            print("store Done")

# Example usage:
if __name__ == "__main__":
    # Define the URL of the CSV file
    url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"

    # Initialize the ETL class with the URL
    etl_processor = MarketingDataETL(url)

    # Extract data
    etl_processor.extract()

    # Transform data (remove NA values and reformat dates)
    etl_processor.transform()

    # Store the transformed data into a CSV file
    etl_processor.store("transformed_marketing_data.csv")

# Read the CSV file into a pandas DataFrame
df = pd.read_csv("transformed_marketing_data.csv")

# Display the DataFrame
print(df)

# Task 2: Inheritance & Polymorphism
import pandas as pd

class MarketingDataETL:
    def __init__(self, url):
        self.url = url
        self.data = None

    def extract(self):
        # Read the CSV file into a pandas DataFrame
        self.data = pd.read_csv(self.url, delimiter=';')

    def transform(self):
        # Drop rows with missing values (NA)
        if self.data is not None:
            self.data.dropna(inplace=True)
            # Convert 'purchase_date' column to datetime format (YYYY-MM-DD)
            self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], format='%d/%m/%y')
            # Reformat 'purchase_date' column to YYYY-MM-DD
            self.data['purchase_date'] = self.data['purchase_date'].dt.strftime('%Y-%m-%d')

    def store(self, output_file):
        # Store the transformed DataFrame to a CSV file
        if self.data is not None:
            self.data.to_csv(output_file, index=False)

class TargetedMarketingETL(MarketingDataETL):
    def segment_customers(self):
        if self.data is not None:
            # Categorize customers based on amount_spent
            bins = [0, 100, 300, 500, float('inf')]
            labels = ['Bronze', 'Silver', 'Gold', 'Platinum']
            self.data['membership_rank'] = pd.cut(self.data['amount_spent'], bins=bins, labels=labels, right=False)

# Example usage:
url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"
etl_processor = TargetedMarketingETL(url)
etl_processor.extract()
etl_processor.transform()
etl_processor.segment_customers()
etl_processor.store("transformed_data.csv")

# Read the CSV file into a pandas DataFrame
df = pd.read_csv("transformed_data.csv")

# Display the DataFrame
print(df)

#Polymorphism
import pandas as pd

class MarketingDataETL:
    def __init__(self, url):
        self.url = url
        self.data = None

    def extract(self):
        # Read the CSV file into a pandas DataFrame
        self.data = pd.read_csv(self.url, delimiter=';')

    def transform(self):
        # Drop rows with missing values (NA)
        if self.data is not None:
            self.data.dropna(inplace=True)
            # Convert 'purchase_date' column to datetime format (YYYY-MM-DD)
            self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], format='%d/%m/%y')
            # Reformat 'purchase_date' column to YYYY-MM-DD
            self.data['purchase_date'] = self.data['purchase_date'].dt.strftime('%Y-%m-%d')

    def store(self, output_file):
        # Store the transformed DataFrame to a CSV file
        if self.data is not None:
            self.data.to_csv(output_file, index=False)

class TargetedMarketingETL(MarketingDataETL):
    def segment_customers(self):
        if self.data is not None:
            # Categorize customers based on amount_spent
            bins = [0, 100, 300, 500, float('inf')]
            labels = ['Bronze', 'Silver', 'Gold', 'Platinum']
            self.data['membership_rank'] = pd.cut(self.data['amount_spent'], bins=bins, labels=labels, right=False)

    def transform(self):
        # Call the parent class's transform method to perform initial transformations
        super().transform()

        # Additional transformation specific to TargetedMarketingETL
        self.segment_customers()

class MarketingDataViewer:
    def display(self, data):
        # Default display method just prints the DataFrame
        print(data)

# URL sumber data
url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"

# Proses ETL menggunakan kelas TargetedMarketingETL
etl_processor = TargetedMarketingETL(url)

# Ekstraksi, transformasi, dan segmentasi pelanggan
etl_processor.extract()
etl_processor.transform()  # Metode transform overridden
etl_processor.store("transformed_data.csv")

# Membaca data hasil transformasi ke dalam DataFrame
df = pd.read_csv("transformed_data.csv")

# Membuat objek MarketingDataViewer
viewer = MarketingDataViewer()

# Menampilkan DataFrame menggunakan polymorphism
viewer.display(df)

