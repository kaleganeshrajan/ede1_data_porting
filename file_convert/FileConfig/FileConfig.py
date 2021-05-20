import os.path
from datetime import datetime
from BucketHandler import BucketCon
import os
import pandas as pd
from simpledbf import Dbf5


class FileConfig:

    def __init__(self, awacslogger):
        self.filePath = None
        self.fileName = None
        self.fileType = None
        self.bucketName = None
        self.destPath = None
        self.tempfile = '/tmp_'
        self._awacslogger = awacslogger
        self._blob = None

    # Properties, Getters and Setters
    # File Path
    @property
    def filepath(self):
        return self.filePath

    @filepath.setter
    def filepath(self, gspath):
        try:
            path = gspath.split('//')  # gs:// seperation
            pathstr = path[1].split('/')
            self.bucketname = pathstr[0]  # bucket name extraction
            self.filename = pathstr[len(pathstr) - 1]  # filename extraction
            self.filePath = '/'.join(pathstr[1:len(pathstr) - 1]) + '/'
        except Exception as e:
            self._awacslogger.error("File path argument incorrect:" + str(e))
        temp = self.filePath.split('/')
        self.tempfile = temp[len(temp) - 2] + '_'

    # File Name
    @property
    def filename(self):
        return self.fileName

    @filename.setter
    def filename(self, file):
        self.fileName = os.path.splitext(file)[0]
        self.filetype = os.path.splitext(file)[1]

    # File Type
    @property
    def filetype(self):
        return self.fileType

    @filetype.setter
    def filetype(self, file):
        self.fileType = file

    # Bucket Name
    @property
    def bucketname(self):
        return self.bucketName

    @bucketname.setter
    def bucketname(self, bucketname):
        self.bucketName = bucketname

    # Destination Path
    @property
    def destpath(self):
        return self.destPath

    @destpath.setter
    def destpath(self, path):
        self.destPath = path

    # Set Config

    def setConfig(self, args):
        self.filepath = args.filePath
        self.destpath = args.destPath

        self._awacslogger.info("File Details -- File :" + self.filePath + self.fileName + self.fileType +
                               "  Bucket Name:" + self.bucketName + "  Destination File:" + self.destPath)

        # Bucket Connection
        try:
            bucketcon = BucketCon.BucketConfig(self._awacslogger)
            bucket = bucketcon.getbucketconn(self.bucketName)
            blob = bucket.get_blob(self.filePath +
                                   self.fileName + self.fileType)
            self._blob = blob
            # Check file empty
            if self._blob == None:
                self._awacslogger.error("Empty file or File does not exist.")
                print("Empty file or File does not exist.")
                exit(-1)
        except Exception as e:
            self._awacslogger.error("Failed to connect Bucket: " + str(e))
            print("Failed to connect Bucket: " + str(e))
            exit(-1)

    # File Convert

    def convert(self):
        # Identify file type and create desired file type parsing object
        if self.fileType == '.xlsx' or self.fileType == '.xls':
            self._awacslogger.info(
                self.fileType + " File type Identified: " + self.fileName + self.fileType)
            # initializing the class
            parser = XLS(self._awacslogger)
        elif self.fileType == '.DBF' or self.fileType == '.dbf':
            self._awacslogger.info(
                self.fileType + " File type Identified: " + self.fileName + self.fileType)
            # initializing the class
            parser = DBF(self._awacslogger, self.tempfile + self.fileName)
        else:
            parser = None
            self._awacslogger.error("File not found or Invalid file type.")
            print("File not found or Invalid file type.")
            return

        # Director object creted
        director = Director(self._awacslogger)

        # Build Parser
        self._awacslogger.info("File porting started: " +
                               self.fileName + self.fileType)
        print("File porting started: " + self.fileName + self.fileType)
        director.setBuilder(parser)  # Setting type of builder
        convert = director.parseFile(self._blob, self)  # Parse method call
        convert.saveConvertedFile(self)  # Saving parsed file
        # Deleting temp file if created
        convert.deleteTempFile(self.tempfile + self.fileName + '.DBF')


# Director class handles builder
class Director:
    __builder = None

    def __init__(self, awacslogger) -> None:
        self._awacslogger = awacslogger

    # Set builder according to file type
    def setBuilder(self, builder) -> None:
        self.__builder = builder

    # Parse file through builder
    def parseFile(self, blob, sourcefile):
        parse = Parser(self._awacslogger)  # Parser object
        try:
            df = self.__builder.convert(blob)
            parse.convertedFile(df)
            self._awacslogger.info(
                "File conversion Done :" + sourcefile.fileName + sourcefile.fileType)
        except Exception as e:
            self._awacslogger.error(
                "File conversion error : " + sourcefile.fileName + sourcefile.fileType + " Error:" + str(e))
            print("File conversion error : " + sourcefile.fileName +
                  sourcefile.fileType + " Error:" + str(e))
            exit(-1)
        return parse


# Parse File
class Parser:
    def __init__(self, awacslogger) -> None:
        self.__df = None
        self._awacslogger = awacslogger

    def convertedFile(self, df) -> None:
        self.__df = df

    def saveConvertedFile(self, sourcefile) -> None:
        # Check df is null
        
        try:
            self.__df.to_csv(sourcefile.destPath, '|',  index=False)
            self._awacslogger.info(
                "Ported file saved at :" + sourcefile.destPath)
            print("Done.")
        except Exception as e:
            self._awacslogger.error(
                "Ported file cannot save at :" + sourcefile.destPath + " ERROR: " + str(e))
            print("Ported file cannot save at :" +
                    sourcefile.destPath + " ERROR: " + str(e))
            exit(-1)

    def deleteTempFile(self, path) -> None:
        if os.path.exists(path):
            try:
                os.remove(path)
                self._awacslogger.info("Temp file deleted at : " + path)
            except Exception as e:
                self._awacslogger.error(
                    "Can't delete file at : " + path + str(e))


# Builder Class
class Builder:

    # builder convert method pass
    def convert(self) -> None: pass


# XLS builder class
class XLS(Builder):

    def __init__(self, awacslogger) -> None:
        self._awacslogger = awacslogger

    # File conversion from xls or xlsx to csv
    def convert(self, blob):
        try:
            df = pd.DataFrame(pd.read_excel(blob.download_as_bytes()))
            return df
        except Exception as e:
            self._awacslogger.error(
                "Data porting for .xls or .xlsx failed:" + str(e))


# DBF builder class
class DBF(Builder):

    def __init__(self, awacslogger, tempFile) -> None:
        self._awacslogger = awacslogger
        self.__tempfile = tempFile

    # File conversion from dbf to csv
    def convert(self, blob):
        try:
            blob.download_to_filename(self.__tempfile + '.DBF')
            dbf = Dbf5(self.__tempfile + '.DBF', codec='utf-8')
            df = dbf.to_dataframe()
            return df
        except Exception as e:
            self._awacslogger.error("Data porting for .dbf failed:" + str(e))
