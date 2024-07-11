import pandas as pd
from connection_str import engine
from log import log_etl
from generate_hash import generate_hash
from generate_uuid import generate_uuid
from log import log_etl
from datetime import datetime


def rename_columns(new_incremental_df):
    columns = [
    'run_id', 'created_at', 'id', 'titleText_text', 'titleType_id', 'certificate_rating', 'releaseDate_year',
    'releaseDate_month', 'releaseDate_day', 'releaseDate_country_text', 'runtime_seconds', 'ratingsSummary_aggregateRating',
    'ratingsSummary_voteCount', 'genres_genres_0_text',
    'genres_genres_1_text', 'genres_genres_2_text', 'genres_genres_3_text', 'genres_genres_4_text',
    'genres_genres_5_text', 'genres_genres_6_text',
    'directors_0_credits_0_name_nameText_text', 'productionBudget_budget_amount',
    'productionBudget_budget_currency', 'worldwideGross_total_amount', 'worldwideGross_total_currency'
    ]

    # Create a new DataFrame with selected columns copy
    renamed_selected_df = new_incremental_df[columns].copy()
    
    
    renamed_selected_df.rename(columns={'id': 'imdb_code','titleText_text': 'title','titleType_id': 'type','releaseDate_year': 'release_year',
                       'releaseDate_month': 'release_month','releaseDate_day': 'release_day','runtime_seconds': 'runtime','ratingsSummary_aggregateRating': 'rating',
                       'ratingsSummary_voteCount': 'votecount','genres_genres_0_text': 'genre0',
                       'genres_genres_1_text': 'genre1','genres_genres_2_text': 'genre2', 'releaseDate_country_text':'country',
                       'genres_genres_3_text': 'genre3','genres_genres_4_text': 'genre4','genres_genres_5_text': 'genre5',
                       'genres_genres_6_text': 'genre6', 'directors_0_credits_0_name_nameText_text':'director',
                       'productionBudget_budget_amount': 'productionBudget_amount','productionBudget_budget_currency': 'productionBudget_currency',
                       'worldwideGross_total_amount': 'worldwideGross_amount','worldwideGross_total_currency': 'worldwideGross_currency'}, inplace=True)

    return renamed_selected_df


def melt_genre_columns(renamed_selected_df):
    melted_df = renamed_selected_df.melt(id_vars=['run_id', 'created_at' , 'imdb_code', 'title', 'type', 'certificate_rating', 'release_year',
                                 'release_month', 'release_day','country','runtime', 'rating', 'votecount', 'director',
                                 'productionBudget_amount', 'productionBudget_currency', 'worldwideGross_amount',
                                 'worldwideGross_currency'],
                        value_vars=['genre0', 'genre1', 'genre2', 'genre3', 'genre4', 'genre5', 'genre6'],
                        var_name='genre_column', value_name='genre')

    # Drop NaN values in the genre column
    melted_df = melted_df.dropna(subset=['genre'])

    # Optional: Drop the genre_column column if you don't need it
    melted_genre_df = melted_df.drop(columns=['genre_column'])
    
    return melted_genre_df


def change_data_type(melted_df):
    # List of columns categorized by their types
    obj_types = ['run_id', 'imdb_code', 'title', 'type', 'certificate_rating', 'country', 'director', 'productionBudget_currency', 'worldwideGross_currency', 'genre']
    datetime_types = ['created_at']
    int_types = ['release_year', 'release_month', 'release_day', 'runtime', 'votecount']
    float_types = ['rating', 'productionBudget_amount', 'worldwideGross_amount']
    
    for column in melted_df.columns:
        if column in obj_types:
            melted_df[column] = melted_df[column].astype('object')
        elif column in datetime_types:
            # assign the result back to the DataFrame column because pd.to_datetime returns a Series object. 
            melted_df[column] = pd.to_datetime(melted_df[column])
        elif column in int_types:
            melted_df[column] = melted_df[column].astype('int64')
        elif column in float_types:
            melted_df[column] = melted_df[column].astype('float64')
    
    return melted_df


def handle_null_values(data_type_changed_df):
    # List of columns categorized by their types
    mean_impute = ['rating']
    zero_impute = ['productionBudget_amount', 'worldwideGross_amount']
    forward_fill_impute = ['release_year', 'release_month', 'release_day']
    median_impute = ['runtime']
    mode_impute = ['productionBudget_currency', 'worldwideGross_currency', 'genre']
    unknown_impute = ['title', 'type', 'country', 'director']
    
    # Impute missing values based on the defined strategies
    for column in data_type_changed_df.columns:
        if column in mean_impute:
            data_type_changed_df[column] = data_type_changed_df[column].fillna(data_type_changed_df[column].mean())
        
        elif column in zero_impute:
            data_type_changed_df[column] = data_type_changed_df[column].fillna(0)
        
        elif column in forward_fill_impute:
            data_type_changed_df[column] = data_type_changed_df[column].ffill()
        
        elif column in median_impute:
            data_type_changed_df[column] = data_type_changed_df[column].fillna(data_type_changed_df[column].median())
        
        elif column in mode_impute:
            data_type_changed_df[column] = data_type_changed_df[column].fillna(data_type_changed_df[column].mode().iloc[0])
        
        elif column in unknown_impute:
            if data_type_changed_df[column].dtype == 'object':
                data_type_changed_df[column] = data_type_changed_df[column].fillna('Unknown')
    
    # Handling special case for certificate_rating
    data_type_changed_df['certificate_rating'] = data_type_changed_df['certificate_rating'].fillna('Not Rated')
    
    return data_type_changed_df


def create_dimension_table(df, columns, id_column=None, hash_function=None):
    dim_table = df[columns].drop_duplicates()
    dim_table.reset_index(inplace=True, drop=True)
    
    if id_column and hash_function:
        dim_table[id_column] = dim_table.apply(hash_function, axis=1)
    
    return dim_table


def create_fact_table(df, dim_tables, merge_keys, drop_columns):
    # Merge with each dimension table
    for dim_table, merge_key in zip(dim_tables, merge_keys):
        df = df.merge(dim_table, left_on=merge_key, right_on=merge_key, how='inner')
    
    # Drop specified columns
    df.drop(columns=drop_columns, inplace=True)
    
    # Generate unique row IDs
    df['row_id'] = df.apply(lambda _: generate_uuid(), axis=1)
    
    return df

# main code that runs other func
def transform():
    try:
        start_time = datetime.now()
        
        # pull run_id
        run_id_query = """SELECT distinct run_id
                FROM staging_table 
                where created_at = (
                    select max(created_at) as max_created_at
                    FROM staging_table
                )"""   
        
        # get data from staging table incrementally
        query = """SELECT * 
                FROM staging_table 
                where created_at = (
                    select max(created_at) as max_created_at
                    FROM staging_table
                )"""
        
        # Fetch data
        new_incremental_df = pd.read_sql(query, con=engine)
        
        run_id_df = pd.read_sql(run_id_query, con=engine)
        run_id = run_id_df['run_id'].iloc[0]
        
        # Rename columns
        renamed_selected_df = rename_columns(new_incremental_df)
        
        # melt genre df : combine multiple genre columns to one and explode them using melt not using explode
        melted_genre_df = melt_genre_columns(renamed_selected_df)
        
        # convert runtime to minutes from seconds
        melted_genre_df['runtime']= melted_genre_df['runtime'].apply(lambda x:x/60)
        
        # need to handle null value first before converting data types
        # null value handle
        null_handled_df = handle_null_values(melted_genre_df)

        # convert the data types
        data_type_changed_df = change_data_type(null_handled_df)
        

        # create dimension tables
        dim_genre = create_dimension_table(data_type_changed_df, ['genre'], 'genre_id', generate_hash)
        dim_director = create_dimension_table(data_type_changed_df, ['director'], 'director_id', generate_hash)
        dim_country = create_dimension_table(data_type_changed_df, ['country'], 'country_id', generate_hash)
        dim_details = create_dimension_table(data_type_changed_df, ['imdb_code', 'title', 'type', 'certificate_rating'])
        
        # create fact table
        dim_tables = [dim_details, dim_genre, dim_country, dim_director]
        merge_keys = ['imdb_code', 'genre', 'country', 'director']
        
        drop_columns = ['title_x', 'type_x', 'certificate_rating_x', 'country', 'director', 
                    'productionBudget_currency', 'worldwideGross_currency', 'genre', 
                    'title_y', 'type_y', 'certificate_rating_y']

        # Create the fact table using the function
        fact_movie = create_fact_table(data_type_changed_df, dim_tables, merge_keys, drop_columns)
        
        end_time = datetime.now()
        log_etl(run_id, 'transform', 'success', count_rows=len(data_type_changed_df), start=start_time, end = end_time)
        
        return dim_genre, dim_director, dim_country, dim_details, fact_movie
    
    except Exception as e:
        log_etl(run_id,'save_to_staging_area', 'failed', error_msg=str(e))
        return None

if __name__ == '__main__':
    
    transform()