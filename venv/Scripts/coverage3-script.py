#!C:\Users\FAKHRI\PycharmProjects\DCM-ADMIN\venv\Scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'coverage==4.4.2','console_scripts','coverage3'
__requires__ = 'coverage==4.4.2'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('coverage==4.4.2', 'console_scripts', 'coverage3')()
    )
