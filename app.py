from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
import json
from dynamic_loader import DynamicAPILoader
from config import db_config
import os
import csv
from io import StringIO
import re
import tempfile
from datetime import timedelta
import psycopg2
from psycopg2.extras import Json

app = Flask(__name__)
app.secret_key = "supersecretkey"
app.permanent_session_lifetime = timedelta(hours=1)


def save_data_to_temp_file(data):
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json')
    json.dump(data, temp_file)
    temp_file.close()
    return temp_file.name

def make_row_hashable(row):
    return tuple(sorted((k, str(v) if v is not None else '') for k, v in row.items()))

def get_postgres_type(value):
    if value is None:
        return 'TEXT'
    elif isinstance(value, dict) or isinstance(value, list):
        return 'JSONB'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, int):
        return 'BIGINT'
    elif isinstance(value, float):
        return 'DOUBLE PRECISION'
    elif isinstance(value, str):
        try:
            int(value)
            return 'BIGINT'
        except ValueError:
            try:
                float(value)
                return 'DOUBLE PRECISION'
            except ValueError:
                return 'TEXT'
    else:
        return 'TEXT'

def convert_value_for_postgres(value):
    if value is None:
        return None
    elif isinstance(value, (dict, list)):
        return Json(value)
    elif isinstance(value, (int, float, str, bool)):
        return value
    else:
        return str(value)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        api_url = request.form.get('api_url')
        table_name = request.form.get('table_name').lower().replace(' ', '_')
        table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            flash("Table name must start with a letter or underscore and contain only letters, numbers, and underscores.", "error")
            return redirect(url_for('index'))
        json_file = request.files.get('json_file')
        data = None
        if not table_name:
            flash("Please provide a Table Name", "error")
            return redirect(url_for('index'))
        if api_url:
            loader = DynamicAPILoader(db_config)
            try:
                data = loader.fetch_api_data(api_url)
            except Exception as e:
                flash(f"Error fetching API: {e}", "error")
                return redirect(url_for('index'))
            finally:
                loader.close()
        elif json_file and json_file.filename:
            try:
                file_content = json_file.read()
                data = json.loads(file_content)
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list):
                            data = v
                            break
                    else:
                        data = [data]
                elif not isinstance(data, list):
                    flash("Unsupported JSON structure", "error")
                    return redirect(url_for('index'))
            except Exception as e:
                flash(f"Error reading JSON file: {e}", "error")
                return redirect(url_for('index'))
        else:
            flash("Please provide either an API URL or upload a JSON file", "error")
            return redirect(url_for('index'))
        if not data or not isinstance(data, list) or not data:
            flash("No data found to preview", "error")
            return redirect(url_for('index'))
        temp_file_path = save_data_to_temp_file(data)
        session['temp_file_path'] = temp_file_path
        session['table_name'] = table_name
        return render_template('preview.html', data=data[:10], table_name=table_name, columns=list(data[0].keys()))
    return render_template('index.html')

@app.route('/preview/confirm', methods=['POST'])
def preview_confirm():
    try:
        table_name = session.get('table_name')
        temp_file_path = session.get('temp_file_path')
        if not table_name or not temp_file_path:
            flash("Session expired. Please try again.", "error")
            return redirect(url_for('index'))
        columns = request.form.getlist('columns')
        if not columns:
            flash("No columns selected", "error")
            return redirect(url_for('index'))
        rename_map = {}
        for col in columns:
            new_name = request.form.get(f'rename_{col}', col)
            rename_map[col] = new_name
        remove_duplicates = request.form.get('remove_duplicates') == 'on'
        trim_whitespace = request.form.get('trim_whitespace') == 'on'
        loader = DynamicAPILoader(db_config)
        try:
            with open(temp_file_path, 'r') as f:
                data = json.load(f)
            total_rows = len(data)
            chunk_size = 1000
            total_count = 0
            seen = set() if remove_duplicates else None
            for i in range(0, total_rows, chunk_size):
                chunk = data[i:i + chunk_size]
                cleaned_chunk = []
                for row in chunk:
                    new_row = {}
                    for col in columns:
                        val = row.get(col, None)
                        if trim_whitespace and isinstance(val, str):
                            val = val.strip()
                        val = convert_value_for_postgres(val)
                        new_row[rename_map[col]] = val
                    if remove_duplicates:
                        row_key = make_row_hashable(new_row)
                        if row_key not in seen:
                            seen.add(row_key)
                            cleaned_chunk.append(new_row)
                    else:
                        cleaned_chunk.append(new_row)
                if cleaned_chunk:
                    if i == 0:
                        with loader.conn.cursor() as cur:
                            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
                            columns_def = []
                            for col_name, sample_value in cleaned_chunk[0].items():
                                col_type = get_postgres_type(sample_value)
                                columns_def.append(f'"{col_name}" {col_type}')
                            create_table_sql = f'''
                                CREATE TABLE "{table_name}" (
                                    {', '.join(columns_def)}
                                );
                            '''
                            cur.execute(create_table_sql)
                            loader.conn.commit()
                    with loader.conn.cursor() as cur:
                        columns = list(cleaned_chunk[0].keys())
                        placeholders = ','.join(['%s'] * len(columns))
                        insert_sql = f'''
                            INSERT INTO "{table_name}" ({','.join(f'"{col}"' for col in columns)})
                            VALUES ({placeholders})
                        '''
                        values = [[row[col] for col in columns] for row in cleaned_chunk]
                        cur.executemany(insert_sql, values)
                        loader.conn.commit()
                        count = len(cleaned_chunk)
                        total_count += count
                flash(f"Processed {min(i + chunk_size, total_rows)} of {total_rows} rows...", "info")
            flash(f"Successfully inserted {total_count} records into table '{table_name}'", "success")
        except Exception as e:
            flash(f"Error: {str(e)}", "error")
        finally:
            loader.close()
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        session.pop('temp_file_path', None)
        session.pop('table_name', None)
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    loader = DynamicAPILoader(db_config)
    num_tables = 0
    total_rows = 0
    recent_tables = []
    try:
        table_list = loader.list_tables()
        num_tables = len(table_list)
        row_counts = []
        for table in table_list:
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{table}";')
                count = cur.fetchone()[0]
                row_counts.append((table, count))
        total_rows = sum(count for _, count in row_counts)
        with loader.conn.cursor() as cur:
            cur.execute("""
                SELECT relname FROM pg_class WHERE relkind='r' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname='public')
                ORDER BY relfilenode DESC LIMIT 5;
            """)
            recent_tables = [row[0] for row in cur.fetchall()]
    finally:
        loader.close()
    return render_template('dashboard.html', num_tables=num_tables, total_rows=total_rows, recent_tables=recent_tables)

@app.route('/tables')
def tables():
    loader = DynamicAPILoader(db_config)
    try:
        table_list = loader.list_tables()
    finally:
        loader.close()
    return render_template('tables.html', tables=table_list)

@app.route('/tables/<table_name>')
def view_table(table_name):
    loader = DynamicAPILoader(db_config)
    try:
        with loader.conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}" LIMIT 100;')
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        loader.close()
    return render_template('view_table.html', table_name=table_name, columns=columns, rows=rows)

@app.route('/tables/<table_name>/export')
def export_table_csv(table_name):
    loader = DynamicAPILoader(db_config)
    try:
        with loader.conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}";')
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
    finally:
        loader.close()
    si = StringIO()
    cw = csv.writer(si)
    cw.writerow(columns)
    cw.writerows(rows)
    output = si.getvalue()
    return send_file(
        StringIO(output),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{table_name}.csv'
    )

if __name__ == '__main__':
    app.run(debug=True)
