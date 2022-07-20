#read files from winSCP and create a data frame
#......Following will be create......
#......ParketItem
#......VendorMaster
#......OpenItem
#......ClosedItem

import paramiko
import os
import pandas as pd

host = 'sftp-gw.gpsemea.ihost.com'
username = 'ibmkrkana001'
remote_folder_path = '\\ANALYTICS\\WCO'
password = 'L2Csftp@frankfurt'
    #keyfilepath = '\\Users\\BaijnathKumar\\Documents\\Project\\WCA\\sftp_private_key.pem'
keyfilepath = 'C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\sftp_private_key.pem'
filepath = '/ANALYTICS/WCO/RAW'

class winSCPappend:  #append winSCP files

    def __init__(self,host,username,remote_folder_path,password,keyfilepath,filepath):
        self.host=host
        self.username=username
        self.remote_folder_path=remote_folder_path
        self.password=password
        self.keyfilepath=keyfilepath
        self.filepath=filepath


    def appendFile(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=self.username, password=self.password, key_filename=self.keyfilepath)
        sftp = ssh.open_sftp()

        closedList = []
        OpenList = []
        VendorList = []
        ParkedList = []
        readDone = []
        readNotDone = []
        closedListread = []
        openListread = []
        venderListread = []
        ParkedListread = []
        colum_closed = set()
        colum_open = set()
        colum_vendor = set()
        colum_parked = set()

        for file in sftp.listdir(filepath):
            try:
                with sftp.open(filepath + '/' + file, ) as f:
                    f.prefetch()

                    if 'ClosedItems' in file:
                        print("ClosedItems:-", file)
                        df = pd.read_csv(f, sep='|', error_bad_lines=False)

                        if '-----BEGIN PGP MESSAGE-----' in df.columns:
                            continue
                        else:
                            colum_closed = df.columns
                            closedListread.append(file)
                        for line in df.values:
                            closedList.append(line)


                    elif 'OpenItems' in file:
                        print("OpenItems:-", file)
                        df = pd.read_csv(f, sep='|', error_bad_lines=False)

                        if '-----BEGIN PGP MESSAGE-----' in df.columns:
                            continue
                        else:
                            colum_open = df.columns
                            openListread.append(file)
                        for line in df.values:
                            OpenList.append(line)


                    elif 'VendorMaster' in file:
                        print("VendorMaster:-", file)
                        df = pd.read_csv(f, sep='|', error_bad_lines=False)

                        if '-----BEGIN PGP MESSAGE-----' in df.columns:
                            continue
                        else:
                            colum_vendor = df.columns
                            venderListread.append(file)
                        for line in df.values:
                            VendorList.append(line)


                    elif 'ParkedItems' in file:
                        print("ParkedItems:-", file)
                        df = pd.read_csv(f, sep='|', error_bad_lines=False)

                        if '-----BEGIN PGP MESSAGE-----' in df.columns:
                            continue
                        else:
                            colum_parked = df.columns
                            ParkedListread.append(file)
                        for line in df.values:
                            ParkedList.append(line)

                    readDone.append(file)
            except:
                readNotDone.append(file)

        dfclosedList = pd.DataFrame(closedList, columns=colum_closed)
        dfOpenItem = pd.DataFrame(OpenList, columns=colum_open)
        dfVendorList = pd.DataFrame(VendorList, columns=colum_vendor)
        dfParkedList = pd.DataFrame(ParkedList, columns=colum_parked)

        dfclosedList.to_csv('C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\output\\ClosedItem.csv')
        dfOpenItem.to_csv('C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\output\\OpenItem.csv')
        dfVendorList.to_csv('C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\output\\VendorMaster.csv')
        dfParkedList.to_csv('C:\\Users\\01934L744\\Box\\Baijnath Data\\Project\\WCA\\output\\ParketItem.csv')
        print("List of successful ", readDone)
        print("List of not successful ", readNotDone)
        print("cloded read:--", closedListread)
        print("open read:--", openListread)
        print("vendor read:--", venderListread)
        print("parke item:--", ParkedListread)

        return readDone,readNotDone





data=winSCPappend( host,username,remote_folder_path,password,keyfilepath,filepath  )
data.appendFile()