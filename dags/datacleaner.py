def data_cleaner():
    import pandas as pd
    import re

    df = pd.read_csv('/home/binisha/airflow/store_files/raw_store_transactions.csv')

    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(clean_store_location)
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(clean_product_id)

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(remove_dollar)

    df.to_csv('/home/binisha/airflow/store_files/cleaned.csv', index=False)

# 🔽 This line is very important!
if __name__ == '__main__':
    data_cleaner()
