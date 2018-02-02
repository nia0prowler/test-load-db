import os
import sys

print("Found Python version: {}".format(sys.version))

tools_path = 'D:\\home\\site\\tools'
if not sys.version.startswith('3.6'):

    # in python 2.7
    import urllib
    print('Installing Python Version 3.6.3')

    from zipfile import ZipFile

    if not os.path.exists(tools_path):
        os.makedirs(tools_path)
        print("Created [{}]".format(tools_path))

    python_url = 'https://apmlstor.blob.core.windows.net/wheels/python361x64.zip'
    python_file = os.path.join(tools_path, 'python.zip')
    urllib.urlretrieve(python_url, python_file)
    print("Downloaded Python 3.6.3")

    python_zip = ZipFile(python_file, 'r')
    python_zip.extractall(tools_path)
    python_zip.close()
    print("Extracted Python to [{}]".format(tools_path))

    print("Please rerun this function again to install required pip packages")
    sys.exit(0)

# in python 3.6
print("Installing packages")

import pip

def install_package(package_name):
    pip.main(['install', package_name])

install_package('cryptography')
install_package('azure-batch')
install_package('azure-storage')
install_package('pyodbc')

print("Setup is finished")