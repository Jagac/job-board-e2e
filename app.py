from datetime import datetime
import os

mydir = os.getcwd()
mydir_tmp = mydir + "//src"
today = datetime.today().strftime('%Y-%m-%d')

print(f'{today}: [ Welcome to the app! ]')
operating_system = input("Enter your operating system: ")

os.system('python -m venv env')

if operating_system == "Windows":
    os.system('env\\Scripts\\activate')
else:
    os.system('source .env/bin/activate')
    
os.system("pip install -r requirements.txt")

print(f'{today}: [ Starting pipeline ]')
os.system(f"{mydir_tmp}/main_pipeline.py")

print(f'{today}: [ Starting dash app ]')
os.system(f"{mydir_tmp}/dash_app.py")