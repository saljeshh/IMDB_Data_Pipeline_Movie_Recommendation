import hashlib
import pandas as pd

def generate_hash(value):
    if isinstance(value, pd.Series):
        # If value is a Series, convert it to a single concatenated string
        value_str = '|'.join(value.astype(str))
        return hashlib.md5(value_str.encode()).hexdigest()
    else:
        # Otherwise, assume value is a single string or numeric value
        return hashlib.md5(str(value).encode()).hexdigest()
