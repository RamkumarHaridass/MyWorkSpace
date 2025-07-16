import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from locust import HttpUser, task
import os
import json
from datetime import datetime
import random
import zipfile

tokenDict = {
    'SeniorRisk' : '' ,
    'Performer'  : '',
    'Approver'   : ''
}
fromDate = "2025-05-08"
toDate = "2025-05-14"
authorInput = ''
userType = "Permer"
rDate = "2025-05-09"
baseDir = r"C:\"

reports = {
    "ABC": {"id": "27100", "expected_count": 3},
    "C": {"id": "27119", "expected_count": 3},
    "D": {"id": "27078", "expected_count": 51},
    "E": {"id": "26704", "expected_count": 4},
    "F": {"id": "27143", "expected_count": 46},
    "G": {"id": "27020", "expected_count": 1},
    "H": {"id": "24551", "expected_count": 2},
    "I": {"id": "27080", "expected_count": 9},
}
def listAllFilesInDir(dir,fileNameAlone = 'True'):
    filesList = []
    zeroKbFiles = []
    dirCount = 0
    for path, subdirs, files in os.walk(dir):
        dirCount += len(subdirs)
        for name in files:
            file_path = os.path.join(path, name)
            try:
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    zeroKbFiles.append(name)
                if fileNameAlone == 'True':
                    filesList.append(name)
                else:
                    filesList.append(file_path)
            except OSError:
                print(f"Could not access file: {file_path}")
                continue
    return filesList, len(filesList), dirCount, zeroKbFiles, len(zeroKbFiles)

def renameInvalidFiles(directory):
    """Rename files in the directory if the file name contains 'Invalid'."""
    for file_name in os.listdir(directory):
        if "Invalid" in file_name:
            old_file_path = os.path.join(directory, file_name)
            new_file_name = file_name.replace("Invalid", "replaced")
            new_file_path = os.path.join(directory, new_file_name)
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {old_file_path} -> {new_file_path}")

def getTimeSlice(inputDate):
    """Get the time slice for the given date."""
    dateObj = datetime.strptime(inputDate, "%Y-%m-%d")
    day = dateObj.strftime("%d").lstrip("0")
    month = dateObj.strftime("%b")
    return f"{day} {month}"

def getRandomDateFolder(baseDir):
    """Get a random date folder from the base directory."""
    dateFolders = [os.path.join(baseDir, d) for d in os.listdir(baseDir) if os.path.isdir(os.path.join(baseDir, d))]
    if not dateFolders:
        raise FileNotFoundError(f"No date folders found in directory: {baseDir}")
    return random.choice(dateFolders)

def getRandomFile(directory):
    """Get a random file from the specified directory."""
    renameInvalidFiles(directory)
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        raise FileNotFoundError(f"No files found in directory: {directory}")
    return os.path.join(directory, random.choice(files))

class protonPerformance_1(HttpUser):
    def get_random_token(self):
        return random.choice(list(tokenDict.values()))

    @task
    def appProtonBusinessDate(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/calendar/getProtonBusinessDate", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appGetKeyRing(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/permissions/getKeyRing", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appGetReportMeta(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/report/meta", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appGetStaticDocsMeta(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/docs/meta", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appGetScenariosProcesses(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/processes/", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appGetScenariosProcessMeta(self):
        token = self.get_random_token()
        head = {'accessToken': f'{token}'}
        response = self.client.get("/endPoint/processes/meta", headers=head, verify=False)
        print(response.text, response.status_code)

    @task
    def appReportFilter(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "dateRange": {
                "from": f"{fromDate}",
                "to": f"{toDate}"
            },
            "reportIds": [],
            "reportTypes": []
        }
        response = self.client.post("/endPoint/report/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

    @task
    def appReportFilterSingle(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "dateRange": {
                "from": f"{toDate}",
                "to": f"{toDate}"
            },
            "reportIds": [],
            "reportTypes": []
        }
        response = self.client.post("/endPoint/report/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

    @task
    def appAdjustmentFilter(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "range": {
                "from": f"{fromDate}",
                "to": f"{toDate}"
            },
            "includeArchived": "false",
            "types": ["Manual", "Pencil"]
        }
        response = self.client.post("/endPoint/input/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

    @task
    def appAdjustmentFilterSingle(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "range": {
                "from": f"{toDate}",
                "to": f"{toDate}"
            },
            "includeArchived": "false",
            "types": ["Manual", "Pencil"]
        }
        response = self.client.post("/endPoint/input/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

    @task
    def appDocsFilter(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "types": [2, 3, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27],
            "indLight": True
        }
        response = self.client.post("/endPoint/docs/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

    @task
    def appDocsFilterSingle(self):
        token = self.get_random_token()
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            "types": [2],
            "indLight": True
        }
        response = self.client.post("/endPoint/docs/filter", headers=head, json=payload, verify=False)
        print(response.text, response.status_code)

class appAdjustmentUpload(HttpUser):
    @task
    def upload_adjustment(self):
        url = "/endPoint/input/create/upload"

        print(f"********************************User Start********************************")
        dateFolder = getRandomDateFolder(baseDir)
        if random.choice(["Manual", "Pencil"]) == "Manual":
            manualDir = os.path.join(dateFolder, "Manual")
            filePath = getRandomFile(manualDir)
            uploadType = "Manual"
        else:
            pencilDir = os.path.join(dateFolder, "Pencil")
            filePath = getRandomFile(pencilDir)
            uploadType = "Pencil"
        print(f"File picked for Upload: {filePath}")
        timeslice = getTimeSlice(reportingDate)
        dateTimeStampT = datetime.today().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        try:
            with open(filePath, "rb") as f:
                token = tokenDict[userType]
                head = {'accessToken': f'{token}'}
                response = self.client.post(url, headers=head, files={'file': f}, verify=False)

                if response.status_code == 200:
                    uploadedFileName = response.json().get('filename', 'NA')
                    print(f"Uploaded File Name: {uploadedFileName}")
                else:
                    print(f"ERROR: File upload failed with status code {response.status_code}")
                    print(f"Response: {response.text}")
                    return

        except FileNotFoundError:
            print(f"ERROR: File not found: {filePath}")
            return
        except Exception as e:
            print(f"ERROR: An exception occurred: {e}")
            return
        data = {
            "actionLog": [
                {
                    "action": "uploaded",
                    "dateTimestamp": f"{dateTimeStampT}",
                    "user": f"{authorInput}"
                }
            ],
            "approver": "",
            "author": f"{authorInput}",
            "dateRange": {
                "from": f"{reportingDate}",
                "to": ""
            },
            "dateTimeStamp": f"{dateTimeStampT}",
            "description": f"{uploadedFileName}",
            "filename": f"{uploadedFileName}",
            "state": "Pending",
            "timeSlices": [f"{timeslice}"],
            "type": f"{uploadType}"
        }

        payload = json.dumps(data)
        createUrl = url.replace('create/upload', 'create1')
        head = {'accessToken': f'{token}', 'content-type': 'application/json'}
        response = self.client.post(createUrl, data=payload, headers=head, verify=False)

        if response.status_code != 200:
            print(f"ERROR: Adjustment creation failed with status code {response.status_code}")
            print(f"Response: {response.text}")

        print(f"********************************User End********************************")

class protonPerformance(HttpUser):
    @task
    def appReportDownload(self):
        token = tokenDict['Performer']
        head = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        reportName, reportDetails = random.choice(list(reports.items()))
        reportId = reportDetails["id"]
        expectedFileCount = reportDetails["expected_count"]
        payload = {
            "dateRange": {
                "from": "",
                "to": ""
            },
            "reportIds": [
                reportId
            ],
            "reportTypes": [
                reportName
            ]
        }

        print(f"********************************User Start********************************")
        print(f"Downloading report: {reportName} (ID: {reportId}) with expected file count: {expectedFileCount}")


        response = self.client.post("/endPoint/report/download", headers=head, json=payload, verify=False)
        outputResponse = response.content


        fileName = f"{reportName}_{random.randint(1, 500)}"
        outputDir = r'C:\\locust_download'
        outputpath = fr'{outputDir}\\{fileName}.zip'
        print(f"Saving file to: {outputpath}")

        with open(outputpath, 'wb') as outf:
            outf.write(outputResponse)
        print(f"File saved: {outputpath}")
        print(f"Response status code: {response.status_code}")

        extractDir = fr'{outputDir}\\{fileName}_extracted'
        os.makedirs(extractDir, exist_ok=True)
        with zipfile.ZipFile(outputpath, 'r') as zip_ref:
            zip_ref.extractall(extractDir)
        print(f"File extracted to: {extractDir}")

        filesList, actualFileCount,dirCount, zeroKbFiles, zeroKbLength = listAllFilesInDir(extractDir)
        print(f"Total files in directory '{extractDir}': {actualFileCount}")
        if actualFileCount == expectedFileCount:
            print(f"SUCCESS: File count matches for report '{reportName}' (Expected: {expectedFileCount}, Actual: {actualFileCount}). Directory Count : {dirCount} Zero KB Files : {zeroKbLength}")
        else:
            print(f"ERROR: File count matches for report '{reportName}' (Expected: {expectedFileCount}, Actual: {actualFileCount}). Directory Count : {dirCount} Zero KB Files : {zeroKbLength}")

        print("********************************User End********************************")

 