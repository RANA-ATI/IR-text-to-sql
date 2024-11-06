import pandas as pd
from sqlalchemy import create_engine
import sqlite3
from groq import Groq
import os
import re

class DataExtractor:
    def __init__(self, file_path, selected_columns):
        self.file_path = file_path
        self.selected_columns = selected_columns
        self.data = self.load_data()

    def load_data(self):
        data = pd.read_csv(self.file_path, delimiter=';')
        if not all(col in data.columns for col in self.selected_columns):
            raise ValueError("One or more specified columns are not in the dataset")
        return data[self.selected_columns]

    def convert_to_snake_case(self, name):
        return name.replace(" ", "_").lower()

    def preprocess_data(self):
        self.data.columns = [self.convert_to_snake_case(col) for col in self.data.columns]
        self.data.insert(0, 'row_id', range(1, len(self.data) + 1))
        return self.data

    def save_to_csv(self, output_path):
        self.preprocess_data().to_csv(output_path, index=False)

    def save_to_sql(self, db_path):
        engine = create_engine(f'sqlite:///{db_path}')
        self.data.to_sql('products', con=engine, index=False, if_exists='replace')


class QueryExecutor:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
    
    def make_df(self,response):
        # Convert response into a DataFrame
        df = pd.DataFrame(response['Rows'], columns=response['Column Names'])
        return df
    
    def df_to_custom_json(self,df):
        row_ids = df["row_id"].tolist()  # Collect all row_ids
        json_list = []

        # Loop through columns except 'row_id'
        for col in df.columns:
            if col == "row_id":
                continue
            unique_values = df[col].unique().tolist()  # Get unique values for each column
            json_item = {
                "column_name": col,
                "value": unique_values,
                "row_ids": row_ids
            }
            json_list.append(json_item)

        return json_list
    def execute_query(self, query):
        table_columns = ['row_id', 'order_id', 'order_date', 'product_category', 'customer_name']
        cursor = self.connection.cursor()
        cursor.execute(query)    
        results = cursor.fetchall()
        
        if not results:
            return {"message": "No data found for the query"}

        # Use regex to extract column names, including those with functions
        matched_columns = re.findall(r'SELECT\s+(.+?)\s+FROM', query, re.IGNORECASE)
        
        if matched_columns:
            # Split and clean up the matched columns
            matched_columns = matched_columns[0].split(',')
            matched_columns = [col.strip() for col in matched_columns]

            # Strip out SQL functions and keep only the column names
            cleaned_columns = []
            for col in matched_columns:
                # Remove SQL functions (like MAX, MIN, etc.) and just keep the column name
                cleaned_col = re.sub(r'^\w+\s*\(\s*(\w+)\s*\)', r'\1', col)  # e.g., MAX(order_id) becomes order_id
                cleaned_col = cleaned_col.strip()
                if cleaned_col in table_columns:
                    cleaned_columns.append(cleaned_col)

            # Ensure the column order matches the SQL query order
            df = self.make_df({'Column Names': cleaned_columns, 'Rows': results})
            
            return self.df_to_custom_json(df)

        return {"message": "No valid columns found in the query"}

    # def execute_query(self, query):
    #     table_columns = ['row_id','order_id', 'order_date', 'product_category', 'customer_name']
    #     cursor = self.connection.cursor()
    #     cursor.execute(query)    
    #     results = cursor.fetchall()
    #     if not results:
    #         return {"message": "No data found for the query"}
        
    #     # Final column names list: retain only valid matches from the cursor description
    #     matched_columns = set(re.findall(r'\b\w+\b', query.split("FROM")[0])) & set(table_columns)
    #     column_names = list(matched_columns)
    #     ordered_column_names = sorted(column_names, key=lambda col: table_columns.index(col))
    #     df = self.make_df({'Column Names':ordered_column_names,'Rows':results})
    #     return self.df_to_custom_json(df)
    

class SQLGenerator:
    def __init__(self, api_key, schema):
        self.client = Groq(api_key=api_key)
        self.schema = schema

    def build_sql_query(self,user_input):
        prompt = f"""
        ### Task Description:
        - You are to transform user-provided SQL schema information into a valid SQL query.
        - Only convert input that describes schema components; respond with an error message for any non-schema related input.
        - Identify relevant columns from varied input formats for query construction.
        - Only adehere to output format provided in ### Output format section by only providing the executable sql query and nothing extra text.
        - Do not use inverted commas on table name in the generated sql query.
        - Handle the synonyms and typographical errors in user input. for example. Clothes is same as Apparel in product_category column. 
        - Always select row_id column and the column name specified in the user query. Do not select any other extra column name.
        - Do not generate any query if the User input is empty or irrelevant.

        Some examples:
            - User input: [‘apparel product’]
            - Sql Query: "SELECT row_id,product_category FROM products WHERE product_category = 'Apparel'"
            - User input: [‘cosmetics’]
            - Sql Query: "SELECT row_id,product_category FROM products WHERE product_category = 'Cosmetics & Personal Care'"
            - User input: "Show all orders placed on 2023-07-01 for Electronics."
            - Sql Query: "SELECT row_id,product_category,order_date FROM products WHERE order_date = '2023-07-01' AND product_category = 'Electronics';"
            - user_input: "heighest order id"
            - Sql Query: "SELECT row_id, MAX(order_id) FROM products"
            - User input: "Show all orders placed by MobileMax between 2023-06-01 and 2023-07-31."
            - Sql Query: "SELECT row_id,customer_name,order_date FROM products WHERE customer_name = 'MobileMax' AND order_date BETWEEN '2023-06-01' AND '2023-07-31';"
            - User input:  "all orders except those for clothes and Groceries."
            - Sql Query: "SELECT row_id,product_category FROM products WHERE product_category NOT IN ('Apparel', 'Groceries');"
            - User input: ""
            - Sql Query: ""
  
        ### User Input:
        Generate a SQL query that answers the question `{user_input}`.

        ### Schema
        This query will run on a database whose schema is represented in this string:
        {self.schema}

        ### Task:
        Construct a SQL query based on the provided user input that interacts with the above schema.

        ### Output format:
        
                [Insert your SQL query here based on the user input and schema.]

        """
        return prompt

    def generate_content(self, prompt):

        response = self.client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are an advanced sql database expert"},
                {"role": "user", "content": prompt}
            ],
            # model="llama-3.1-70b-versatile"
            model="llama3-groq-70b-8192-tool-use-preview"
        )
        return response.choices[0].message.content


def main(user_queries):
    # Parameters
    file_path = 'take_home_dataset.csv'
    selected_columns = ['Order_ID', 'Order_date', 'Product_Category', 'Customer_Name']
    output_csv_path = "data/products.csv"
    db_path = 'data/products.db'
    api_key = "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"

    # Data extraction and storage
    if not os.path.exists(db_path):
        extractor = DataExtractor(file_path, selected_columns)
        extractor.save_to_csv(output_csv_path)
        extractor.save_to_sql(db_path)

    # SQL query execution
    executor = QueryExecutor(db_path)
    schema = """
        CREATE TABLE "products" (
        "row_id" INTEGER,-- Sample values like: [1, 2, 3, 4, 5]
        "order_id" INTEGER,-- Sample values like: [3808, 487, 4080, 4868, 5251]
        "order_date" TEXT,-- Sample values like: [2023-07-01]
        "product_category" TEXT,-- Sample values like: [Apparel, Cosmetics & Personal Care, Groceries, Toys & Games, Electronics]
        "customer_name" TEXT-- Sample values like: [ElecHouse, MobileMax, AeroTechs, ElegantEyes, Cust-040]
        )
    """
    
    # Generate SQL query
    generator = SQLGenerator(api_key, schema)

    results = []
    for user_input in user_queries:
        prompt = generator.build_sql_query(user_input)
        sql_query = generator.generate_content(prompt)
        print(sql_query)

        # Execute the query
        if not sql_query or not user_input.strip():
            json_item = {
                "column_name": None,
                "value": None,
                "row_ids": None
            }
            results.append([json_item])
            continue
        try:
            raw_data = executor.execute_query(sql_query)
            print(raw_data)
            results.append(raw_data)
        except Exception as e:
            print(f"Error executing {sql_query}: {e}")
            # Append empty JSON if there's an error
            sql_query = generator.generate_content(prompt)
            try:
                raw_data = executor.execute_query(sql_query)
                results.append(raw_data)
            except Exception as e:
                print(f"Again Error executing {sql_query}: {e}")
                json_item = {
                    "column_name": None,
                    "value": None,
                    "row_ids": None
                }
                results.append([json_item])

    return results


if __name__ == "__main__":
    # user_input = ["largest order no","cosmetics and clothes","apparel products","cosmetics and personal care products", "order ID greater than 4000."," "]
    user_input = ["product name and consumer name with lowest order id"]
    results = main(user_input)
    print(results)
