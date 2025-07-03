import requests
import zipfile
import os

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
zip_path = "data.zip"
extract_to = "source"

print("📥 Downloading archive...")
response = requests.get(url)
with open(zip_path, "wb") as f:
    f.write(response.content)

print(f"📦Extracting to folder'{extract_to}'...")
os.makedirs(extract_to, exist_ok=True)
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_to)

print("🧹 Deleting archive...")
os.remove(zip_path)

print(f"✅ Done! Archive extracted to folder '{extract_to}'.")

