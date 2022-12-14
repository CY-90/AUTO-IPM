import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from datetime import datetime
from os.path import exists
from ipm_open_server import OpenServer
import subprocess
import os
import shutil
import EMSDB
import pandas as pd
import cx_Oracle
import AutoIPM as ipm

###START: user specified input data
start_time = '2022-12-10 00:00:00'
end_time = '2022-12-11 00:00:00'
Corr = "PetroleumExperts5" # VLP Correlation
path = r'C:\Users\lcyan01\Desktop\Assets\Angola\SnO\WTA\Python Code Test' #to be modified - folder where models are stored
server_name = 'ANGLUAKN1' #PI historian server
database = 'ANGSDB' # EMSDB Database
###END: user specified input data

#initialize
ipm.initalize(server_name, database)

#well input data
wellTags = pd.read_csv(path + "/Well Status Table PI Tags.csv")
wellData = ipm.GetWellInputData(start_time, end_time, wellTags, data_dir=path)

#manifold data
ManifoldTags = pd.read_csv(path + "/ManifoldTags.csv")
manifoldData = ipm.GetManifoldInputData(start_time, end_time, ManifoldTags, data_dir=path)

#prosper IPR data
IPRData = ipm.GetIPRFromProsper(data_dir = path)

#Get Latest Well Test Data
wellTestData = ipm.GetWellTest(end_time, wellData, data_dir=path)

#Tune Prosper Model
ipm.tuneProsperModel(wellTestData, Corr, data_dir=path)

GapFile = "KizA GAP_20Mar2019_with_latest_well_tests_tuned" #Gap Model to be used
ipm.OpenGAPModel(GapFile, path) # Open Gap Model
ipm.UpdateGAPModel(IPRData, wellData, manifoldData, data_dir = path) #Update GAP Model to reflect data specified within the period
ipm.TuneManifoldPressureDrop(manifoldData) #Tune Pipeline pressure drop
BaseResult = ipm.GetCalculatedOutput(wellData, data_dir=path, fileString= "baseResult.csv") # Get GAP model calculated result
ipm.OptimizeGapModel(wellData, 320) #Optimize GAP Model 
OptResult = ipm.GetCalculatedOutput(wellData, data_dir=path, fileString= "optResult.csv") # Get GAP model calculated result (optimized)

