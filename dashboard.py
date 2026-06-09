# dashboard.py - Redirect to app.py (v5)
import os, runpy
os.environ.setdefault('STREAMLIT_SERVER_HEADLESS', 'true')
runpy.run_path('app.py', run_name='__main__')
