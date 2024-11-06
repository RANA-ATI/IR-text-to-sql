# app.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os

# Importing classes from text_to_sql.py
from text_to_sql import DataExtractor, QueryExecutor, SQLGenerator

# Initialize the FastAPI app
app = FastAPI()

# Set up constants and initial configurations
file_path = 'take_home_dataset.csv'
selected_columns = ['Order_ID', 'Order_date', 'Product_Category', 'Customer_Name']
output_csv_path = "data/products.csv"
db_path = 'data/products.db'
api_key = "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"
schema = """
        CREATE TABLE "products" (
        "row_id" INTEGER,-- Sample values like: [1, 2, 3, 4, 5]
        "order_id" INTEGER,-- Sample values like: [3808, 487, 4080, 4868, 5251]
        "order_date" TEXT,-- Sample values like: [2023-07-01]
        "product_category" TEXT,-- Sample values like: [Apparel, Cosmetics & Personal Care, Groceries, Toys & Games, Electronics]
        "customer_name" TEXT-- Sample values like: [ElecHouse, MobileMax, AeroTechs, ElegantEyes, Cust-040]
        )
    """

# Initialize SQL Generator and Data Extractor on server start
generator = SQLGenerator(api_key, schema)

if not os.path.exists(db_path):
    extractor = DataExtractor(file_path, selected_columns)
    extractor.save_to_csv(output_csv_path)
    extractor.save_to_sql(db_path)

executor = QueryExecutor(db_path)


class QueryInput(BaseModel):
    queries: List[str]



@app.get("/")
async def read_root():
    return {"message": "Welcome to the SQL Query API"}

@app.post("/query")
async def generate_and_execute_sql(queries: QueryInput):
    try:
        results = []
        for user_input in queries.queries:
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
        return {"results": results}
    
    except Exception as e:
        return {"results":f"Exception occured while executing query. Here are the details {e}"}

