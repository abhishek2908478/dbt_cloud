#!c:\users\10672078\desktop\dbt-demo\demo_dbt\env\scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'snowflake-connector-python==2.2.10','console_scripts','snowflake-export-certs'
__requires__ = 'snowflake-connector-python==2.2.10'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('snowflake-connector-python==2.2.10', 'console_scripts', 'snowflake-export-certs')()
    )
