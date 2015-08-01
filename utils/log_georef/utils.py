__author__ = 'Alex'
import os
class Utils():
    def __init__(self):
        return
    @staticmethod
    def initOutFld(flds,outDir):
        """
        make folders output folders
        :return:
        """
        # create output folder devided by device
        f = lambda fld: os.mkdir(outDir + "\\" + fld)
        # check if folder already exist
        e = lambda fld: os.path.isdir(outDir + "\\" + fld)
        # create folders if need
        [f(d) for d in flds if e(d) == False]