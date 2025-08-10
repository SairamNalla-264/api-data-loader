# api-data-loader
<<<<<<< HEAD
# api-data-loader
=======
# Dynamic API to PostgreSQL Loader

A web application to dynamically fetch data from any API or upload a JSON file, preview and clean the data, customize columns, and load it into a PostgreSQL database. Includes a dashboard, table browser, and export to CSV functionality.

## Features
- **API Fetch & JSON Upload:** Fetch data from any API or upload a JSON file.
- **Data Preview:** Preview the data before inserting into the database.
- **Column Customization:** Select, rename, and sanitize columns before saving.
- **Data Cleaning:** Remove duplicates and trim whitespace before saving.
- **Table Browser:** Browse all tables in your database and view their contents.
- **Dashboard:** See summary stats (number of tables, total rows, recent tables).
- **Export to CSV:** Download any table as a CSV file.

## Requirements
- Python 3.7+
- PostgreSQL
- pip (Python package manager)

## Setup
1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd api-data-loader
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure the database:**
   - Edit `config.py` with your PostgreSQL credentials:
     ```python
     db_config = {
         'dbname': 'api_data',
         'user': 'postgres',
         'password': 'yourpassword',
         'host': 'localhost',
         'port': 5432
     }
     ```
   - Make sure the database exists:
     ```bash
     createdb api_data
     ```

## Running the App
```bash
python app.py
```
Visit [http://localhost:5000](http://localhost:5000) in your browser.

## Usage
### Home Page
- Enter an API URL **or** upload a JSON file.
- Enter a table name (letters, numbers, underscores, must start with a letter or underscore).
- Click **Fetch/Upload & Store**.

### Data Preview & Cleaning
- Preview the first 10 rows of your data.
- Select/deselect columns, rename them, and choose cleaning options (remove duplicates, trim whitespace).
- Click **Confirm & Insert** to save to the database.

### Table Browser
- Click **Table Browser** in the top navigation.
- See a list of all tables. Click any table to view its contents (up to 100 rows).
- Use **Export to CSV** to download the table.

### Dashboard
- Click **Dashboard** in the top navigation.
- See the number of tables, total rows, and recent tables.

## Screenshots
> _Add screenshots of the main pages here for better documentation._

## Notes
- Table names are sanitized to prevent SQL errors.
- Only JSON arrays or objects containing arrays are supported for upload.
- Data is limited to 50 rows for preview and 100 rows for table browsing for performance.

## License
MIT 
>>>>>>> b2f4082 (Initial commit)
