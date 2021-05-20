#! /usr/bin/env python3
from FileConfig import FileConfig
from AwacsLogger import AwacsLogger
import argparse


if __name__ == '__main__':

    # Logger File
    awacslogger = AwacsLogger.AWACSLogger("Xls_Dbf_to_CSV").logger

    # Default Argument
    parser = argparse.ArgumentParser(description='File Porting Arguments.')
    # Adding Arguments with flags
    # Path of File
    parser.add_argument('-p', '--path', dest='filePath', type=str, default=None,
                        help='for input file path')
    # Destination File Path
    parser.add_argument('-d', '--dpath', dest='destPath', type=str, default=None,
                        help='for destination file path' )

    args = parser.parse_args()

    # check all required argument given or not
    if not args.filePath or not args.destPath:
        awacslogger.error("Invalid arguments")
        print(parser.print_help())
        exit(-1)
    # Setting File Details and bucket conn
    sourcefile = FileConfig.FileConfig(awacslogger)
    sourcefile.setConfig(args)
    sourcefile.convert()

# py .\ede_xls_dbf_to_csv.py -p gs://balatestawacs/SampleFiles/AIOCD0923/AIOCD0923_02_2021_511b9d2d-76c3-4e4e-a2a4-35840fc612ce.xls --dpath /Tempfiles2/pqr.csv
# py .\ede_xls_dbf_to_csv.py -p gs://balatestawacs/SALE_DTL.DBF --dpath /Tempfiles3/p/abcd.csv
# py .\ede_xls_dbf_to_csv.py -p "gs://balatestawacs/SampleFiles/StockSales/STANDARD V4 PATCH/2021/Mar/Month/AIOCD2046_8a414eff-0943-4eaa-8816-83cf5625b06a/dist_dtl.DBF" --dpath /Temp/abcd.csv
