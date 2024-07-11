from extract import save_to_staging_area
from transform import transform
from load import load

def main():
    try:
        # Extract
        save_to_staging_area()
        print('EXTRACTION COMPLETED STAGED Complete ✅')

    except Exception as e:
        print(f"Extraction failed⭕: {str(e)}")
        return  # Exit the function or handle the error as needed

    try:
        # Transform
        dim_genre, dim_director, dim_country, dim_details, fact_movie = transform()
        print('TRANSFORMATION COMPLETED ✅')

    except Exception as e:
        print(f"Transformation failed⭕: {str(e)}")
        return  # Exit the function or handle the error as needed

    try:
        # Load
        load(dim_genre, dim_director, dim_country, dim_details, fact_movie)
        print('LOAD COMPLETED ✅')

    except Exception as e:
        print(f"Load failed⭕: {str(e)}")
        return  # Exit the function or handle the error as needed

if __name__ == "__main__":
    main()
