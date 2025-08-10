import requests
import psycopg2
import psycopg2.extras

class DynamicAPILoader:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)

    def fetch_api_data(self, api_url):
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        # Handle if data is a list or dict with key holding list
        if isinstance(data, dict):
            # Try to find first list in values
            for v in data.values():
                if isinstance(v, list):
                    return v
            # fallback to dict as list
            return [data]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError("Unsupported JSON structure")

    def generate_sql_type(self, value):
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "TEXT"

    def create_table(self, table_name, sample_record):
        columns = []
        for key, value in sample_record.items():
            if key.lower() == 'id':  # Skip id because we add SERIAL PRIMARY KEY ourselves
                continue
            col_type = self.generate_sql_type(value)
            col_name = key.lower().replace(' ', '_')
            columns.append(f"{col_name} {col_type}")
        columns_sql = ", ".join(columns)
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns_sql});"

        with self.conn.cursor() as cur:
            cur.execute(create_sql)
            self.conn.commit()

    def insert_data(self, table_name, data):
        if not data:
            return 0
        # Filter out 'id' key
        columns = [k for k in data[0].keys() if k.lower() != 'id']
        cols_lower = [c.lower().replace(' ', '_') for c in columns]

        query = f"""
            INSERT INTO {table_name} ({', '.join(cols_lower)})
            VALUES %s
            ON CONFLICT DO NOTHING;
        """

        values = []
        for record in data:
            values.append(tuple(record.get(col, None) for col in columns))

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, values)
            self.conn.commit()
        return len(values)

    def list_tables(self):
        query = """
            SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname = 'public';
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            tables = [row[0] for row in cur.fetchall()]
        return tables

    def close(self):
        self.conn.close()
