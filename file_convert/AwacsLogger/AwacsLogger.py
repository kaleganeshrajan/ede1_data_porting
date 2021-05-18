import logging


class AWACSLogger:

    logger = None

    def __init__(self, loggername, filename='AWACS_logs.log', mode='a', fmt='%(asctime)s %(name)-8s %(levelname)s-%(message)s', datefmt='%Y-%m-%d %H:%M:%S') -> None:
        self.logger = logging.Logger(loggername) # Setting logger name
        self.logger.setLevel(logging.DEBUG) # Setting logs level
        self.__file_handler = logging.FileHandler(filename) # Setting  file name
        self.logger.addHandler(self.__file_handler) # adding file handler to logger
        self.__formatter = logging.Formatter(fmt) # Formatter
        self.__file_handler.setFormatter(self.__formatter) # Setting formatter
        self.__file_handler.mode = mode # Mode of file

    # Change Format
    def setLogger(self, loggername) -> None:
        self.logger = logging.Logger(loggername) # Setting logger name

    # Change Filename
    def setFilename(self, filename) -> None:
        self.__file_handler = logging.FileHandler(filename) #File Name

    # Change Format
    def setFormat(self, fmt) -> None:
        self.__formatter = logging.Formatter(fmt)  # Formatter

    # Change Level
    def setLevel(self, level) -> None:
        if level == 'INFO':
            self.logger.setLevel(logging.INFO)
        elif level == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        elif level == 'CRITICAL':
            self.logger.setLevel(logging.CRITICAL)
        elif level == 'ERROR':
            self.logger.setLevel(logging.ERROR)
        elif level == 'WARN':
            self.logger.setLevel(logging.WARN)
        elif level == 'FATAL':
            self.logger.setLevel(logging.FATAL)
