import pandas as pd
import json

def convert_csv_to_json(input_file, output_file):
    """
    Converts a CSV file to a JSON file for VESPA ingestion.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to the output JSON file.
    """

    # Read the CSV file using pandas
    df = pd.read_csv(input_file)

    # Make sure we have the necessary columns in the CSV
    required_columns = ['review_id', 'text', 'rating']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"CSV file must contain the following columns: {required_columns}")

    # Fill missing values with empty strings
    for col in ['text', 'rating']:
        df[col] = df[col].fillna('')

    # Create the 'title' column as a string from 'rating'
    df['title'] = df['rating'].astype(str)

    # Create the 'doc_id' column from 'review_id'
    df['doc_id'] = df['review_id']

    # Use 'text' as the content for the text field
    df['text'] = df['text']

    # Select the necessary columns
    df = df[['doc_id', 'title', 'text']]

    # Create the 'fields' column (this will contain the actual document data)
    df['fields'] = df.apply(lambda row: row.to_dict(), axis=1)

    # Create the 'put' column, which uniquely identifies each document
    df['put'] = df['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

    # Prepare the final output: 'put' and 'fields' columns
    df_result = df[['put', 'fields']]

    # Output the result to a JSON file in line-delimited format
    df_result.to_json(output_file, orient='records', lines=True)

    print(f"Converted data has been saved to {output_file}")

# Example usage
convert_csv_to_json("yelp_reviews.csv", "vespa_yelp_reviews.jsonl")
