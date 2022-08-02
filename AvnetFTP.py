import shutil
import pandas as pd
import paramiko
import logging
import os
from io import BytesIO
import pgpy
import datetime

Current_Date = datetime.datetime.today()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('Logs ' + str(Current_Date.date()) + '.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

# file_handler.setLevel(logging.DEBUG)

class avnetSFTP:
    _connection = None

    apArchive = 'AP_Archive/'
    AP_Decrypted = 'AP_Decrypted/'
    AP_NotDecrypted = 'AP_NotDecrypted/'
    AP_RAW = 'RAW/'
    Ar_Archive = 'Ar_Archive/'
    Ar_Decrypted = 'Ar_Decrypted/'
    Ar_NotDecrypted = 'Ar_NotDecrypted/'
    Ar_RAW = 'Ar_RAW/'

    def __init__(self,host, username, password, keyfilepath,rmtPth,locPth):
        self.host = host
        self.username = username
        self.password = password
        self.keyfilepath = keyfilepath
        self.rmtPth = rmtPth
        self.locPth = locPth
        self.deleteDirAndFile(locPth)
        logger.info("Deleted old files and folder and Re-created folders in local drive")
        self.LocalCheckOrCreateFolder(locPth)
        self.createConnection(self.host, self.username, self.password, self.keyfilepath)
        self.RMTcheckOrCreateFolder(rmtPth)


    def deleteDirAndFile(self,locPath):   # delete folder and files from local
        for dir in os.listdir(locPth):
            shutil.rmtree(locPth + dir)

    def RMTcheckOrCreateFolder(self, rmtPth): # check remote folder if folder missing create

        dirList = [self.apArchive, self.AP_Decrypted, self.AP_NotDecrypted, self.AP_RAW, self.Ar_Archive, self.Ar_Decrypted, self.Ar_NotDecrypted, self.Ar_RAW]
        for di in dirList:
            dirpath = rmtPth + di
            # print(dirpath)
            try:
                self._connection.chdir(dirpath)
            except IOError:
                self._connection.mkdir(dirpath)
                self._connection.chmod(dirpath, mode=0o777)
        logger.info("All folder structure available at SFTP ")

    def LocalCheckOrCreateFolder(self, locPth): #check local folder if missing create
        dirList = [self.apArchive, self.AP_Decrypted, self.AP_NotDecrypted, self.AP_RAW, self.Ar_Archive,
                   self.Ar_Decrypted, self.Ar_NotDecrypted, self.Ar_RAW]
        for di in dirList:
            dirpath = locPth + di
            if not os.path.exists(dirpath):
                os.mkdir(dirpath)
            else:
                pass
                # print(f"{dirpath} already exist")
        logging.info("All folder structure available at Local ")



    @classmethod
    def createConnection(cls, host, username, password, keyfilepath):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, password=password, key_filename=keyfilepath)
        cls._connection = ssh.open_sftp()
        logger.info("Connection created ")



    def download_AP(self, rmtPth, locPth):  #download AP data
        # print('rmtpth:-', rmtPth)
        # print('locPth:-', locPth)
        for file in self._connection.listdir(rmtPth):
            typeFile = ['ClosedItems', 'OpenItems', 'ParkedItems', 'VendorMasterDaily']
            filedate, fileDay, previsusDay, Previous_Date = self.checkSunday(file)
            # print(f"Ap file download:-{file}")
            splitFileType = file.split('_')[1]
            dataSource = rmtPth + file
            dataDestination = locPth + file
            if filedate.date() == Previous_Date.date() and previsusDay == 'Sunday' and splitFileType in typeFile:
                try:
                    self._connection.get(dataSource, dataDestination)
                    logger.info(f"Download AP File :- {file}")
                except:
                    pass


    def download_AR(self,rmtPth, locPth):      #download AR data
        # print('rmtpth:-', rmtPth)
        for file in self._connection.listdir(rmtPth):
            typeFile = ['ARClosedItemDaily', 'AROpenItemDaily', 'CustMstrDaily', 'ARDisputesDaily']
            filedate, fileDay, previsusDay, Previous_Date = self.checkSunday(file)
            # print(f"AR file date:= {filedate} file day:-{fileDay} previous day:-{previsusDay} previous date:-{Previous_Date}")
            splitFileType=file.split('_')[1]
            if filedate.date() == Previous_Date.date() and previsusDay == 'Sunday' and splitFileType in typeFile:
                dataSource = rmtPth + file
                dataDestination = locPth + file
                try:
                    self._connection.get(dataSource, dataDestination)
                    logger.info(f"Download AR File :- {file}")
                except:
                    pass



    @staticmethod
    def decryptFile(fil):  # function to decrypt file
        if fil[-8:] == '.txt.gpg':
            key, _ = pgpy.PGPKey.from_file(pgpKey)
            emsg = pgpy.PGPMessage.from_file(fil)
            # print(fil)

            with key.unlock(pasPhrase):
                d = key.decrypt(emsg).message
                df = pd.read_csv(BytesIO(d), error_bad_lines=False, sep='|')
            return df


    @staticmethod
    def checkSunday(file):  # decrypt if file received on Sunday
        Current_Date = datetime.datetime.today()
        Previous_Date = datetime.datetime.today() - datetime.timedelta(days=1)
        previsusDay = Previous_Date.strftime('%A')

        try:
            strdate = file.split('_')[2][:8]
            m = int(strdate[:2])
            d = int(strdate[2:4])
            y = int(strdate[4:])
            filedate = datetime.datetime(y, m, d)
            fileDay = filedate.strftime('%A')
        except:
            strdate = file.split('_')[3][:8]
            m = int(strdate[:2])
            d = int(strdate[2:4])
            y = int(strdate[4:])
            filedate = datetime.datetime(y, m, d)
            fileDay = filedate.strftime('%A')


        return filedate, fileDay, previsusDay, Previous_Date

    def decryptAndSaveLocal(self, locPth ,pathLocalToSave):
        decrySuccess = []
        decryNotSuccess = []
        # localfilePath = locPth + 'AP_Archive/'
        for fil in os.listdir(locPth):
            # decryptedfilePath = locPth + 'AP_Decrypted/'
            pathtoSave = pathLocalToSave + fil.split('.')[0]+".txt" #path to save encrypted file

            filedate, fileDay, previsusDay, Previous_Date = self.checkSunday(fil)  # will complete on monday
            if fileDay == 'Sunday' and filedate.date() == Previous_Date.date():
                fil = locPth + fil
                try:
                    df = self.decryptFile(fil)

                    df.to_csv(pathtoSave, sep='|')
                    decrySuccess.append(fil)
                    logger.info(f"Decrypted file and saved in local folder name:- {fil} ")
                except:
                    decryNotSuccess.append(fil)


    def uploadDecryptedfile(self,  locPth, rmtPth ): #upload decrypted file

        # localSlocsourcefilePath =  locPth + 'AP_Decrypted/'
        # avnetDrptRmtPath = rmtPth + 'AP_Decrypted/'
        for de in os.listdir(locPth):
            lopath = locPth+de
            destpath = rmtPth+de
            # print("local path:-", lopath)
            # print("remote path:-", destpath)
            self._connection.put(lopath, destpath)
        logger.info(f"upload decrypted file complete :-{rmtPth}")

    def uploadToArchived(self, locPth, rmtPth):  #upload all files from local archived to remote archived
        # localSlocsourcefilePath = locPth + 'AP_Decrypted/'
        # avnetDrptRmtPath = rmtPth + 'AP_Decrypted/'
        for de in os.listdir(locPth):
            self._connection.put(locPth +de , rmtPth + de)
        logger.info(f"Uploaded file to remote archive folder {rmtPth + de}")

if __name__ == '__main__':


    host = 'sftp-gw.gpsemea.ihost.com'
    username = 'ibmkrkana001'
    password = 'L2Csftp@frankfurt'
    keyfilepath = 'C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\sftp_private_key.pem'
    rmtPth = '/ANALYTICS/WCO/'
    locPth = 'C:/Users/01934L744/Box/Baijnath Data/Project/WCA/AVNET/'
    pgpKey = 'C:/Users/01934L744/Box/Baijnath Data/Project/WCA/code V2/ibm_analytics_pgp_secret_key.asc'
    pasPhrase = 'L2Cpgp@analytics'

    abc = avnetSFTP(host, username, password, keyfilepath,rmtPth,locPth)
# ...................... AP Data .....................................................

    abc.download_AP(rmtPth=rmtPth + 'RAW/',locPth = locPth + 'AP_Archive/') # download AP data
    logger.info("AP data download complete")

    pathLocalToSave = locPth + 'AP_Decrypted/'   # Save decrypted file in local folder
    abc.decryptAndSaveLocal(locPth + 'AP_Archive/', pathLocalToSave )   # Decrypt data and save to local
    logger.info("AP file decrypted and saved in local folder")

    abc.uploadDecryptedfile( locPth + 'AP_Decrypted/', rmtPth + 'AP_Decrypted/')     #
    logger.info("AP decrypted file uploaded on SFTP server")

    abc.uploadToArchived(locPth=locPth+'AP_Archive/', rmtPth=rmtPth + 'AP_Archive/') #upload all files to remote
    logger.info("AP files uploaded on SFTP server archive folder")
# ...........................................................................................

# ....................... AR data to local path..................................................
    logger.info(".........AR....................")
    ARrmtPath = '/ANALYTICS/PROCESSED_TEST/'
    abc.download_AR(ARrmtPath, locPth + 'Ar_Archive/')    #download AR data
    logger.info("AR data download complete")

    abc.uploadToArchived(locPth=locPth + 'Ar_Archive/', rmtPth=rmtPth + 'Ar_Archive/')  # upload all files to remote
    logger.info("AR files uploaded on SFTP server archive folder")

    pathLocalToSaveAR = locPth + 'Ar_Decrypted/'   # Save decrypted file in local folder
    abc.decryptAndSaveLocal(locPth + 'Ar_Archive/', pathLocalToSaveAR )   # Decrypt AR data and save to local
    logger.info("AR file decrypted and saved in local folder")

    abc.uploadDecryptedfile(locPth + 'Ar_Decrypted/', rmtPth + 'Ar_Decrypted/')
    logger.info("AR decrypted file uploaded on SFTP server")


