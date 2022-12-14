from pihist import Server
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from datetime import datetime
from os.path import exists
from openpyxl import load_workbook
from ipm_open_server import OpenServer
import subprocess
from scipy.optimize import minimize, Bounds, minimize_scalar
import math
import os
from pathlib import Path
import shutil
import EMSDB
import pandas as pd
import cx_Oracle

server_name = 'ANGLUAKN1'
database = 'ANGSDB'
server = Server(server_name)
db = EMSDB.EMSDB()

#Initialises an 'OpenServer' class
petex = OpenServer()

def initalize(server_name, EMSDB):
    global server
    global database
    server = Server(server_name)
    database = EMSDB

def GetWellInputData(start_time, end_time, welltags, OutputToExcel = True, data_dir = None):

    #dic to store values
    BHP_values = {}
    WHP_values = {}
    WHT_values = {}
    WHPDP_values = {}
    status_values = {}
    routing_values = {}
    date_values = {}
    GLRate_values = {}
    endDate = datetime.fromisoformat(end_time)
    startDate = datetime.fromisoformat(start_time)
    interval = endDate - startDate
    interval = interval.total_seconds()

    #Final list
    BHPs = []
    WHPs = []
    WHTs = []
    WHPDPs = []
    Status = []
    Routing = []
    GLRates = []

    if data_dir is None:
        data_dir = os.getcwd()
    url = "/WellInputData.csv"
    path = data_dir + url
    header = True

    ##NEED TO DROP NAN PI TAGS
    statusTags = welltags["Well Status"].dropna()
    FlowARoutingTags = welltags["Flowline A Routing"].dropna()
    whpTags = welltags["WHP"].dropna()
    whpdpTags = welltags["Choke DP"].dropna()
    whtTags = welltags["WHT"].dropna()
    bhpTags = welltags["BHP"].dropna()
    glRateTags = welltags["GL Rate"].dropna()
    wellNames = list(welltags["Well Name"].values)

    statusData = server.get_tag_interpolated_data(statusTags, start_time, end_time)
    FlowlineAData = server.get_tag_interpolated_data(FlowARoutingTags, start_time, end_time)

    for i in statusData:
        value = 'FLOW' if i[1].values[-1] == 0 else 'NFLOW'
        well = i[0].split("_")[1]
        status_values[well] = value

    for i in FlowlineAData:
        value = 'HEADER_A' if i[1].values[-1] == 1 else 'HEADER_B'
        well = i[0].split("_")[1]
        routing_values[well] = value

    WHPData = server.get_tag_time_averaged_data(whpTags, start_time, end_time, interval_secs= interval)
    WHTData = server.get_tag_time_averaged_data(whtTags, start_time, end_time, interval_secs= interval)
    WHPDPData = server.get_tag_time_averaged_data(whpdpTags, start_time, end_time, interval_secs= interval)
    BHPData = server.get_tag_time_averaged_data(bhpTags, start_time, end_time, interval_secs= interval)
    GLRateData = server.get_tag_time_averaged_data(glRateTags, start_time, end_time, interval_secs= interval)

    for i in WHPData:
        value = 0 if i[1] is None else i[1].Avg.values[0] * 14.5038
        well = i[0].split("_")[1]
        WHP_values[well] = value

    for i in WHTData:
        value = 0 if i[1] is None else i[1].Avg.values[0]* 9/5 + 32
        well = i[0].split("_")[1]
        WHT_values[well] = value

    for i in WHPDPData:
        value = 0 if i[1] is None else i[1].Avg.values[0] * 14.5038
        well = i[0].split("_")[1]
        WHPDP_values[well] = value

    for i in BHPData:
        value = 0 if i[1] is None else i[1].Avg.values[0] * 14.5038
        well = i[0].split("_")[1]
        BHP_values[well] = value

    for i in GLRateData:
        value = 0 if i[1] is None else (i[1].Avg.values[0] * 35.3147 * 24)/(1000 * 1000)
        well = i[0].split("_")[1]
        GLRate_values[well] = value

    for well in wellNames:
        if well in status_values:
            Status.append(status_values[well])
        else:
            Status.append(-999)

        if well in routing_values:
            Routing.append(routing_values[well])
        else:
            Routing.append(-999)

        if well in WHP_values:
            WHPs.append(WHP_values[well])
        else:
            WHPs.append(-999)

        if well in WHT_values:
            WHTs.append(WHT_values[well])
        else:
            WHTs.append(-999)

        if well in WHPDP_values:
            WHPDPs.append(WHPDP_values[well])
        else:
            WHPDPs.append(-999)

        if well in BHP_values:
            BHPs.append(BHP_values[well])
        else:
            BHPs.append(-999)

        if well in GLRate_values:
            GLRates.append(GLRate_values[well])
        else:
            GLRates.append(-999)

    date_values = [end_time] * len(wellNames)

    Result = {'Date' : date_values, 'WellName' : wellNames, 'Status' : Status,
            'WHP': WHPs, 'BHP': BHPs, 'WHT': WHTs, 'WHPDP': WHPDPs, 'GLRate' : GLRates,
            'Routing' : Routing}
    df = pd.DataFrame(Result, columns = ['Date', 'WellName', 'Status', 'WHP', 'BHP', 'WHT', 'WHPDP', 'GLRate', 'Routing'])
    
    if OutputToExcel:
        df.to_csv(path, index = False, header = header)

    return df

def GetMPFMRates(start_time, end_time, welltags, OutputToExcel = True, data_dir = None):

    date_values = []
    OilRate_values = []
    WaterRate_values = []
    GasRate_values = []
    endDate = datetime.fromisoformat(end_time)
    startDate = datetime.fromisoformat(start_time)
    interval = endDate - startDate
    interval = interval.total_seconds()

    if data_dir is None:
        data_dir = os.getcwd()
    url = "/MPFMInputData.xlsx"
    path = data_dir + url
    header = True
    file_exists = exists(path)

    if file_exists:
        header = False
    
    oilRateTags = welltags["MPFM Oil Rates"]
    waterRateTags = welltags["MPFM Water Rates"]
    gasRateTags = welltags["MPFM Gas Rates"]
    wellNames = welltags["Well Name"]
    OilRateData = server.get_tag_time_averaged_data(oilRateTags, start_time, end_time, interval_secs= interval)
    WaterRateData = server.get_tag_time_averaged_data(waterRateTags, start_time, end_time, interval_secs= interval)
    GasRateData = server.get_tag_time_averaged_data(gasRateTags, start_time, end_time, interval_secs= interval)

    for i in OilRateData:
        value = 0 if i[1] is None else i[1].Avg.values[0]
        OilRate_values.append(value * 6.2981 * 24)
        date_values.append(startDate)

    for i in WaterRateData:
        value = 0 if i[1] is None else i[1].Avg.values[0]
        WaterRate_values.append(value * 6.2981 * 24)

    for i in GasRateData:
        value = 0 if i[1] is None else i[1].Avg.values[0]
        GasRate_values.append(value * 35.4147 * 24 / 1000000)

    Result = {'Date' : date_values, 'WellName' : wellNames,
            "OilRate" : OilRate_values, "WaterRate" : WaterRate_values, "GasRate" : GasRate_values}
    df = pd.DataFrame(Result, columns = ['Date', 'WellName', 'OilRate', 'WaterRate', 'GasRate'])
    
    if OutputToExcel:
        df.to_excel(url, index = False, header = header)

    return df
    
def GetReservoirPressure(end_time, wellName):
    db = EMSDB.EMSDB()
    resPressure_values = []
    end_time = end_time.split()[0]
    qq = """SELECT b.completion_name, a.test_date, a.analyzed_pressure_at_gauge * 0.145038, a.mid_perf_pressure * 0.145038
        from rpm.reservoir_pressure_published a left join eg.ofm_completion b on a.id_completion = b.id_completion
        where a.test_date <= TO_DATE('""" + end_time + """', 'yyyy-mm-dd')
        order by b.completion_name, a.test_date desc"""
    df = db.query(qq, database)

    for well in wellName:

        #wellnamestr = well[0:3] + "-" + well[3:]
        df1 = df[df["COMPLETION_NAME"] == well]
        midPerfPressure = df1.values[0][3] if df1.values.size > 0 else 0.0
        resPressure_values.append(midPerfPressure)
    return resPressure_values

def GetWellTest(end_time, wellData, OutputToExcel = True, data_dir = None):

    wellName = list(wellData["WellName"].values)
    wellNames = []
    oilRates = []
    waterRates = []
    gasRates = []
    gasLiftRates = []
    FWHPs = []
    FWHTs = []
    FBHPs = []
    WellTestDates = []
    end_time = end_time.split()[0]
    qq = """SELECT b.completion_name, a.test_usage, a.start_date, a.oil_rate * 6.289, a.water_rate * 6.289, a.assoc_gas_rate * 35.3147/1000/1000,
        a.glg_rate * 35.3147/1000/1000, a.flowing_bhp * 0.145038, a.flowing_wellhead_pressure * 0.145038, a.flowing_wellhead_temp * 9/5 + 32
        from eg.well_test_prod a left join eg.ofm_completion b on a.id_completion = b.id_completion
        where a.start_date <= TO_DATE('""" + end_time + """', 'yyyy-mm-dd') and a.test_usage = 'Allocation'
        order by b.completion_name, a.start_date desc"""
    db = EMSDB.EMSDB()
    df = db.query(qq, database)

    for well in wellName:

        df1 = df[df["COMPLETION_NAME"] == well]
        if df1.values.size > 0:
            startDate = df1.values[0][2]
            oilRate = df1.values[0][3]
            waterRate = df1.values[0][4]
            gasRate = df1.values[0][5]
            gasliftRate = df1.values[0][6]
            FBHP = df1.values[0][7]
            FWHP = df1.values[0][8]
            FWHT = df1.values[0][9]
            completionName = df1.values[0][0]

            WellTestDates.append(startDate)
            oilRates.append(oilRate)
            waterRates.append(waterRate)
            gasRates.append(gasRate)
            gasLiftRates.append(gasliftRate)
            FBHPs.append(FBHP)
            FWHPs.append(FWHP)
            FWHTs.append(FWHT)
            wellNames.append(completionName)

    resPressures = GetReservoirPressure(end_time, wellName)
    Result = {'WellName' : wellNames, 'Date' : WellTestDates, "OilRate" : oilRates, "WaterRate" : waterRates, "GasRate" : gasRates,
             "GasLiftRate" : gasLiftRates, 'WHP': FWHPs, 'BHP': FBHPs, 'WHT': FWHTs, "ResPressure" : resPressures}   
    df = pd.DataFrame(Result, columns = ['WellName', 'Date', "OilRate", "WaterRate", "GasRate", "GasLiftRate", "WHP", "BHP", "WHT", "ResPressure"])
    if(OutputToExcel):
        if data_dir is None:
            data_dir = os.getcwd()
        url = "/wellTestResult.csv"
        path = data_dir + url
        df.to_csv(path, header = True)

    return df

def GetManifoldInputData(start_time, end_time, ManifoldTags, OutputToExcel = True,  data_dir = None):

    values = []

    if data_dir is None:
        data_dir = os.getcwd()

    Tags = list(ManifoldTags["Tag"].values)
    endDate = datetime.fromisoformat(end_time)
    startDate = datetime.fromisoformat(start_time)
    interval = endDate - startDate
    interval = interval.total_seconds()
    Types = []

    ManifoldData = server.get_tag_time_averaged_data(Tags, start_time, end_time, interval_secs= interval)

    for i in ManifoldData:
        value = 0 if i[1] is None else i[1].Avg.values[0]
        values.append(value)

    #Get pressure index
    pressure_index = list(ManifoldTags[ManifoldTags["Type"] == "Pressure"].index)

    for index in pressure_index:
        values[index] = values[index] * 14.5038

    Joints = list(ManifoldTags["Joint"].values)
    Types = list(ManifoldTags["Type"].values)
    Properties = list(ManifoldTags["Property"].values)
    Flowlines = list(ManifoldTags["Flowline"].values)
    Commingled_Flowlines = list(ManifoldTags["Commingled Flowline"].values)
    Pipes = list(ManifoldTags["Pipe"].values)

    Result = {"Joint" : Joints, "Type" : Types, "Property" : Properties, "Flowline" : Flowlines, "Commingled Flowline" :  Commingled_Flowlines, "Pipe" : Pipes, "Value" : values}

    outputPath = data_dir + '/ManifoldInputData.csv'
    header = True

    df = pd.DataFrame(Result, columns = ['Joint', 'Type', 'Property', 'Flowline', 'Commingled Flowline', 'Pipe', 'Value'])
    if OutputToExcel:
        df.to_csv(outputPath, index = False, header = header)

    return df

def tuneProsperModel(WellInputData, corr, OutputToExcel = True, data_dir = None):

    DateList = []
    WellNameList = []
    LiquidRateList = []
    WCUTList = []
    GORList = []
    AvgWHPList = []
    AvgBHPList = []
    AvgWHTList = []
    UValList = []
    CP1ProsperList = []
    CP2ProsperList = []
    CP1SolverList = []
    CP2SolverList = []
    PIList = []
    ResPressureList = []
    CalcLiquidRateSolverList = []
    CalcBHPSolverList = []
    global BHP
    global Corr
    Corr =  corr
    #Creates ActiveX reference and holds a license
    petex.Connect()

    if data_dir is None:
        data_dir = os.getcwd()

    for filename in os.listdir(data_dir):
        if filename.lower().endswith(".out"):
            file_wellName = filename.split("_")[2]
            file_wellName = file_wellName.split(".")[0]
            wellData = WellInputData[WellInputData["WellName"] == file_wellName]

            if wellData.values.size > 0:

                #Perform functions
                ProsperFile = os.path.join(data_dir, filename)
                p1 = subprocess.Popen([ProsperFile],shell = True, stdout =subprocess.PIPE, stderr = subprocess.STDOUT)
                petex.OSOpenFile(ProsperFile,'PROSPER')
                numberOfTests = int(petex.DoGet("PROSPER.ANL.VMT.DATA.COUNT"))
                for j in range(numberOfTests - 1, -1, -1):
                    
                    jstr = str(j)
                    Date = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Date")
                    Comment = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Label")
                    GOR = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].GOR")
                    GORFree = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].GORFree")
                    LiquidRate = petex.DoGet("PROSPER.ANL.VMT.Data[" +  jstr + "].Rate")
                    WCUT = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].WC")
                    WHP = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].THpres")
                    WHT = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].THtemp")
                    BHP = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Gpres")
                    GaugeDepth = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Gdepth")
                    ResPressure = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Pres")
                    GasLiftRate = petex.DoGet("PROSPER.ANL.VMT.Data[" + jstr + "].Irate")

                    jstr = str(j + 1)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Date", Date)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Label", Comment)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].GOR", GOR)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].GORFree", GORFree)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Rate", LiquidRate)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].WC", WCUT)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].THpres", WHP)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].THtemp", WHT)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Gpres", BHP)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Gdepth", GaugeDepth)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Pres", ResPressure)
                    petex.DoSet("PROSPER.ANL.VMT.Data[" + jstr + "].Irate", GasLiftRate)
                    petex.DoSet("PROSPER.ANL.VMT.DATA[" + jstr + "].ENABLE", 1)

                welltestData = wellData
                Date = welltestData["Date"].values[0]
                # Date = Date.strftime("%m/%d/%Y")
                OilRate = welltestData["OilRate"].values[0]
                WaterRate = welltestData["WaterRate"].values[0]
                GasRate = welltestData["GasRate"].values[0]
                GasLiftRate = welltestData["GasLiftRate"].values[0]
                WHP = welltestData["WHP"].values[0]
                BHP = welltestData["BHP"].values[0] if welltestData["BHP"].values[0] > 0.0 else 0
                WHT = welltestData["WHT"].values[0]
                resPressure = welltestData["ResPressure"] .values[0]
                LiquidRate  = OilRate + WaterRate
                WCUT = (WaterRate/LiquidRate) * 100
                GOR = (GasRate*1000000 / OilRate)

                petex.DoSet("PROSPER.ANL.VMT.Data[0].Date", Date)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].GOR", GOR)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].Rate", LiquidRate)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].WC", WCUT)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].THpres", WHP)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].THtemp", WHT)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].Gpres", BHP)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].Irate", GasLiftRate)

                if resPressure > 0:
                    petex.DoSet("PROSPER.ANL.VMT.Data[0].Pres", resPressure)

                else:
                    resPressure = petex.DoGet("PROSPER.SIN.IPR.Single.Pres")

                petex.DoCmd("PROSPER.ANL.VMT.UVAL")

                Uval = float(petex.DoGet("PROSPER.ANL.VMT.Data[0].Uvalue"))
                petex.DoSet("PROSPER.SIN.EQP.Geo.Htc", Uval)
                petex.DoSet("PROSPER.ANL.VMT.Data[0].Uvalue", Uval)

                if BHP > 0 and BHP < resPressure:

                    petex.DoSet("PROSPER.ANL.VMT.CorrLabel[{" + corr + "}]", 1)
                    petex.DoCmd("PROSPER.ANL.VMT.CALC")
                    cp1 = float(petex.DoGet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[0]"))
                    cp2 = float(petex.DoGet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[1]"))
                    #Match VLP
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTRESET(1,5)")
                    petex.DoSet("PROSPER.SIN.EQP.Geo.Htc", Uval)
                    petex.DoSet("PROSPER.ANL.VMT.Data[0].Uvalue", Uval)
                    petex.DoSet("PROSPER.SIN.IPR.Single.Pindex", 100) # helps with convergence
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTCALC(1)")
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTPI(1)")
                    AmendedPI = float(petex.DoGet("Prosper.ANL.VMT.Data[0].PIamend"))

                    #setting amended PI to PI, watercut and GOR used in system analysis
                    petex.DoSet("PROSPER.SIN.IPR.Single.Pindex", AmendedPI)
                    petex.DoSet("PROSPER.SIN.IPR.Single.Pres", resPressure)
                    petex.DoSet("PROSPER.ANL.SYS.WC", WCUT)
                    petex.DoSet("PROSPER.ANL.SYS.GOR", GOR)
                    petex.DoSet("PROSPER.ANL.SYS.TubingLabel", corr)
                    petex.DoSet("PROSPER.ANL.SYS.Pres", WHP)
                    petex.DoSet("PROSPER.SIN.GLF.GLRate", GasLiftRate)
                    petex.DoCmd("PROSPER.ANL.SYS.CALC")

                    CP1_Solver, CP2_Solver = GetProsperCoeff(cp1, cp2)
                    
                    petex.DoSet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[0]", CP1_Solver)
                    petex.DoSet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[1]", CP2_Solver)

                    petex.DoCmd("PROSPER.ANL.SYS.CALC")
                    CalcSolverLiquidRate = float(petex.DoGet("PROSPER.OUT.SYS.Results[0].Sol.LiqRate"))
                    CalcSolverBHP = float(petex.DoGet("PROSPER.OUT.SYS.Results[0].Sol.GaugeP[0]"))

                    #perform VLP calculations and export TPD tables
                    petex.DoSet("PROSPER.ANL.VLP.PipeLabel", Corr)
                    petex.DoSet("PROSPER.ANL.VLP.TubingLabel", Corr)

                    #Generate Sensitivity Cases
                    #Liquid Rate
                    petex.DoSet("PROSPER.ANL.VLP.Sens.Gen.First", 20)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.Gen.Last", 50000)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.Gen.Number", 20)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.Gen.Method", "Geometric Spacing")
                    petex.DoCmd("PROSPER.ANL.VLP.GENRATES")

                    #GOR
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Vars[0]", 17)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[131].Gen.First", 30)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[131].Gen.Last", 25000)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[131].Gen.Number", 10)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[131].Gen.Method", "Geometric Spacing")
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[131].Calc", 17)

                    #WCUT
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Vars[1]", 16)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[6].Gen.First", 0)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[6].Gen.Last", 99)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[6].Gen.Number", 10)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[6].Gen.Method", "Linear Spacing")
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[6].Calc", 16)

                    #Manifold Presuure
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Vars[2]", 27)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Gen.First", 50)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Gen.Last", 2652)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Gen.Number", 10)
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Gen.Method", "Linear Spacing")
                    petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Calc", 27)

                    if GasLiftRate > 0:
                        #GLR injected
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Vars[3]", 23)
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[139].Gen.First", 0)
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[139].Gen.Last", 25600)
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[139].Gen.Number", 10)
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[139].Gen.Method", "Linear Spacing")
                        petex.DoSet("PROSPER.ANL.VLP.Sens.SensDB.Sens[145].Calc", 23)
   

                    petex.DoCmd("PROSPER.ANL.VLP.CALC")
                    path = os.path.join(data_dir, filename.replace(".Out", ".tpd"))
                    petex.DoSet("PROSPER.ANL.VLP.EXP.File", path)
                    petex.DoSet("PROSPER.ANL.VLP.EXP.ExtType", "tpd")
                    petex.DoCmd("PROSPER.ANL.VLP.EXPORTBYEXT")

                else:
                    cp1 = 1.0
                    cp2 = 1.0
                    CP1_Solver = 1.0
                    CP2_Solver = 1.0

                    # petex.DoSet("PROSPER.ANL.VMT.AdjTube", 20) #PE5
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTRESET(1,5)")
                    petex.DoSet("PROSPER.SIN.EQP.Geo.Htc", Uval)
                    petex.DoSet("PROSPER.ANL.VMT.Data[0].Uvalue", Uval)
                    petex.DoSet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[0]", cp1)
                    petex.DoSet("PROSPER.ANL.COR.Corr[{" + corr + "}].A[1]", cp2)
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTCALC(1)")
                    petex.DoSlowCmd("PROSPER.ANL.VMT.ADJUSTPI(1)")
                    AmendedPI = float(petex.DoGet("Prosper.ANL.VMT.Data[0].PIamend"))

                    #setting amended PI to PI, watercut and GOR used in system analysis
                    petex.DoSet("PROSPER.SIN.IPR.Single.Pindex", AmendedPI)
                    petex.DoSet("PROSPER.ANL.SYS.WC", WCUT)
                    petex.DoSet("PROSPER.ANL.SYS.GOR", GOR)
                    petex.DoSet("PROSPER.ANL.SYS.TubingLabel", corr)
                    petex.DoSet("PROSPER.ANL.SYS.Pres", WHP)
                    petex.DoCmd("PROSPER.ANL.SYS.CALC")
                    CalcSolverLiquidRate = float(petex.DoGet("PROSPER.OUT.SYS.Results[0].Sol.LiqRate"))
                    CalcSolverBHP = float(petex.DoGet("PROSPER.OUT.SYS.Results[0].Sol.GaugeP[0]"))

                #storing data
                DateList.append(Date)
                WellNameList.append(file_wellName)
                LiquidRateList.append(LiquidRate)
                WCUTList.append(WCUT)
                GORList.append(GOR)
                AvgWHPList.append(WHP)
                AvgBHPList.append(BHP)
                AvgWHTList.append(WHT)
                UValList.append(Uval)
                CP1ProsperList.append(cp1)
                CP2ProsperList.append(cp2)
                CP1SolverList.append(CP1_Solver)
                CP2SolverList.append(CP2_Solver)
                PIList.append(AmendedPI)
                ResPressureList.append(resPressure)
                CalcLiquidRateSolverList.append(CalcSolverLiquidRate)
                CalcBHPSolverList.append(CalcSolverBHP)

                #Save Prosper models and copy into respective folders
                petex.OSSaveFile(ProsperFile,'Prosper')
                petex.OSCloseFile(ProsperFile,'Prosper')

    petex.Disconnect()
    Result = {'Date' : DateList, 'WellName' : WellNameList, 'LiquidRate': LiquidRateList, 'WCUT': WCUTList, 'GOR': GORList, 
                'PIAvgWHP' : AvgWHPList, 'PIAvgBHP' : AvgBHPList, 'PIAvgWHT' : AvgWHTList,
                'Uval' : UValList, 'CP1_Prosper': CP1ProsperList, 'CP2_Prosper': CP2ProsperList,
                'CP1_Solver': CP1SolverList, 'CP2_Solver': CP2SolverList,  
                'PI' : PIList, 'ResPressure' : ResPressureList, 'CalcLiqRate_Solver' : CalcLiquidRateSolverList, 'CalcBHP_Solver' : CalcBHPSolverList,
                }

    df = pd.DataFrame(Result, columns = ['Date', 'WellName', 'LiquidRate', 'WCUT', 'GOR', 'PIAvgWHP', 'PIAvgBHP',
                                        'PIAvgWHT', 'Uval', 'CP1_Prosper', 'CP2_Prosper', 'CP1_Solver',
                                        'CP2_Solver', 'PI', 'ResPressure', 'CalcLiqRate_Solver', 'CalcBHP_Solver',
                                        ])     

    header = True
    file_exists = exists(data_dir)

    if file_exists:
        header = False
    
    if OutputToExcel:
        df.to_csv(data_dir, mode = 'a', index = False, header = header)

def ObjectiveFunctionProsper(input):

    cp1 = input[0]
    cp2 = input[1]
    measuredDPG = BHP

    petex.DoSet("PROSPER.ANL.COR.Corr[{" + Corr + "}].A[0]", cp1)
    petex.DoSet("PROSPER.ANL.COR.Corr[{" + Corr + "}].A[1]", cp2)
    petex.DoCmd("PROSPER.ANL.SYS.CALC")

    try:
        calcDPG = float(petex.DoGet("PROSPER.OUT.SYS.Results[0].Sol.GaugeP[0]"))
    
    except:
        calcDPG = measuredDPG * 2
        petex.Connect()
    
    result = math.pow(measuredDPG - calcDPG, 2)

    print(result)
    return result

def GetProsperCoeff(cp1, cp2):

    if cp1 >= 0.9 and cp1 <= 1.1 and cp2 >= 0.8 and cp2 <= 1.2:
        return cp1, cp2

    if cp1 < 1.1 and cp2 > 1.2:
        #tries to pass some error to CP1
        initialCP1 = 1.1
        initialCP2 = 1.0
        bounds =[(cp1, 1.1),(1.0, 3.0)]

    elif cp1 < 0.9 and cp2 > 0.8:
        #Tries to improve CP1 score while keeping CP2 above 0.8
        initialCP1 = 0.9
        initialCP2 = 0.8
        bounds =[(cp1, 0.9),(0.8, cp2)]

    elif cp1 > 0.9 and cp2 < 0.8:
        #Tries to improve CP2 score while keeping CP1 above 0.9
        initialCP1 = 0.9
        initialCP2 = 0.8
        bounds =[(0.9, cp1),(cp2, 1.2)]

    elif cp1 > 1.1 and cp2 < 1.2:               
        #tries to pass some error to CP2
        initialCP1 = 1.09
        initialCP2 = 1.2
        bounds =[(0.9, cp1),(cp2, 1.2)]
                
    elif cp1 > 1.1 and cp2 > 1.2 :
        # tries to cap CP1 below 1.1 and allow CP2 to float
        initialCP1 = 1.09
        initialCP2 = 1.2
        bounds =[(1.1, 1.1),(1.2, 3.0)]
        
    else:
        return cp1, cp2

    res = minimize(ObjectiveFunctionProsper, x0 = (initialCP1, initialCP2), method= 'L-BFGS-B', bounds = bounds, options={'ftol' : 0.05, 'tol' : 0.05, 'eps' : 0.001, 'maxiter' : 200, 'verbose': 1, 'disp': True})
    CP1_Solver = res.x[0]
    CP2_Solver = res.x[1] #cp2
    return CP1_Solver, CP2_Solver

def GetIPRFromProsper(OutputToExcel = True, data_dir = None):

    PIList = []
    ResPressureList = []
    GORList = []
    WCUTList = []
    LiquidList = []
    DateList = []
    WellNames = []
    if data_dir is None:
        data_dir = os.getcwd()

    #Creates ActiveX reference and holds a license
    petex.Connect()

    for filename in os.listdir(data_dir):
        if filename.lower().endswith(".out"):
            #Perform functions
            file_wellName = filename.split("_")[2]
            file_wellName = file_wellName.split(".")[0]
            ProsperFile = os.path.join(data_dir, filename)
            p1 = subprocess.Popen([ProsperFile],shell = True, stdout =subprocess.PIPE, stderr = subprocess.STDOUT)
            petex.OSOpenFile(ProsperFile,'PROSPER')
            DateList.append(petex.DoGet('PROSPER.ANL.VMT.Data[0].Date'))
            PIList.append(float(petex.DoGet('PROSPER.SIN.IPR.Single.Pindex')))
            ResPressureList.append(float(petex.DoGet('PROSPER.SIN.IPR.Single.Pres')))
            GORList.append(float(petex.DoGet('PROSPER.ANL.VMT.Data[0].GOR')))
            WCUTList.append(float(petex.DoGet('PROSPER.ANL.VMT.Data[0].WC')))
            LiquidList.append(float(petex.DoGet('PROSPER.ANL.VMT.Data[0].Rate')))
            WellNames.append(file_wellName)

            petex.OSCloseFile(ProsperFile,'PROSPER')

    Result = {'WellName' : WellNames, 'Date' : DateList, 'PI' : PIList, 'ResPressure' : ResPressureList, 'WCUT' : WCUTList, 'GOR' : GORList, 'LiquidRate' : LiquidList }

    df = pd.DataFrame(Result, columns = ['WellName', 'Date', 'PI', 'ResPressure', 'WCUT', 'GOR', 'LiquidRate'])

    path = data_dir + "/ProsperResult.csv"
    header = True
    file_exists = exists(path)

    if file_exists:
        header = False
    
    if(OutputToExcel):
        df.to_csv(path, mode = 'a', index = False, header = header)

    petex.Disconnect()
    return df

def setGAPWellData(inputIPRData, inputWellData, data_dir = None):

    wellNames = list(inputWellData["WellName"].values)

    if data_dir is None:
        data_dir = os.getcwd()

    #update VLP
    for filename in os.listdir(data_dir):
            if filename.lower().endswith(".tpd"):
                file_wellName = filename.split("_")[2]
                file_wellName = file_wellName.split(".")[0]
                wellData = inputWellData[inputWellData["WellName"] == file_wellName] 

                if wellData.values.size > 0: #check if the well exist
                    wellstring = file_wellName[0:3] + '_' + file_wellName[3:]
                    path = os.path.join(data_dir, filename)
                    str1 = "GAP.VLPIMPORT(MOD[{PROD}].WELL[{"+ wellstring +"}], " + '"' + path + '"' + ")"
                    petex.DoGAPFunc(str1)

    #update IPR and operating params
    for well in wellNames:

        wellData = inputWellData[inputWellData["WellName"] == well]
        WHPDP = wellData["WHPDP"].values[0]
        GLRate = wellData["GLRate"].values[0]
        wellstring = well[0:3] + '-' + well[3:] ##temporary
        IPRData = inputIPRData[inputIPRData["WellName"] == wellstring]
        PI = IPRData["PI"].values[0]
        ResPressure = IPRData["ResPressure"].values[0]
        WCUT = IPRData["WCUT"].values[0]
        GOR = IPRData["GOR"].values[0]
        Status = wellData["Status"].values[0]
        wellstring = well[0:3] + '_' + well[3:]

        str1 = "GAP.MOD[{PROD}].WELL[{" + wellstring + "}].IPR[0].PI"
        str2 = "GAP.MOD[{PROD}].WELL[{" + wellstring + "}].IPR[0].ResPres"
        str3 = "GAP.MOD[{PROD}].WELL[{" + wellstring + "}].IPR[0].WCT"
        str4 = "GAP.MOD[{PROD}].WELL[{" + wellstring + "}].IPR[0].GOR"
        str5 = "GAP.MOD[{PROD}].INLCHK[{" + wellstring + "CK""}].DPControlValue"
        str6 = "GAP.MOD[{PROD}].WELL[{" + wellstring + "}].AlqValue"

        petex.DoSet(str1, PI)
        petex.DoSet(str2, ResPressure)
        petex.DoSet(str3, WCUT)
        petex.DoSet(str4, GOR)
        petex.DoSet(str5, WHPDP)
        petex.DoSet(str6, GLRate)

        if Status == 'FLOW':
            petex.DoCmd("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].UNMASK()")

        else:
            petex.DoCmd("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].MASK()")

def setGAPManifoldData(inputManifoldData):

    #Riser GL Rate
    RiserGLData = inputManifoldData[inputManifoldData["Property"] == "Riser GL Rate"]

    #Sep Pressure
    SepPressureData = inputManifoldData[inputManifoldData["Property"] == "Sep Pressure"]

    if RiserGLData.values.size > 0:
        for index, row in RiserGLData.iterrows():
            joint = row["Joint"]
            value = row["Value"]
            petex.DoSet("GAP.MOD[{PROD}].INLINJ[{" + joint + "}].Rate", value)

    if SepPressureData.values.size > 0:
        for index, row in SepPressureData.iterrows():
            joint = row["Joint"]
            value = row["Value"]
            petex.DoSet("GAP.MOD[{PROD}].SEP[{" + joint + "}].SolverPres[0]", value)

def UpdateGAPModel(inputIPRData, inputWellData, inputManifoldData, data_dir = None):

    setGAPWellData(inputIPRData, inputWellData, data_dir)
    setGAPManifoldData(inputManifoldData)

def OpenGAPModel(fileName, data_dir = None):

    petex.Connect()
    if data_dir is None:
        data_dir = os.getcwd()

    #Perform functions
    cwd = 'c:\\Users\\lcyan01\\Desktop\\Assets\\Angola\\SnO\\WTA\\GAP\\Overall\\Feb 14'
    string2 = r'/' + fileName
    string3 = r'.gap'

    GapFile = data_dir + string2 + string3
    p1 = subprocess.Popen([GapFile],shell = True, stdout =subprocess.PIPE, stderr = subprocess.STDOUT)
    petex.OSOpenFile(GapFile,'Gap')

def MaskGAPJoint(Joints):

    for joint in Joints:
        petex.DoCmd("GAP.MOD[{PROD}].JOINT[{" + joint + "}].MASK()")

def UnMaskGAPJoint(Joints):

    for joint in Joints:
        petex.DoCmd("GAP.MOD[{PROD}].JOINT[{" + joint + "}].UNMASK()")

def ObjectiveFunctionPipe(input):
    
    gravityCoef = input[0]
    frictionCoef = input[1]
    result = 0.0

    for pipe in pipes:
        petex.DoSet("GAP.MOD[{PROD}].PIPE[{" + pipe + "}].Matching.AVALS[{Hydro2P}][0]", gravityCoef)
        petex.DoSet("GAP.MOD[{PROD}].PIPE[{" + pipe + "}].Matching.AVALS[{Hydro2P}][1]", frictionCoef)

    petex.DoGAPFunc('GAP.SOLVENETWORK(0)')

    Calc_US_Pressure = float(petex.DoGet("GAP.MOD[{PROD}].JOINT[{" + US_Joint + "}].SolverResults[0].Pres"))
    Calc_DS_Pressure = float(petex.DoGet("GAP.MOD[{PROD}].JOINT[{" + DS_Joint + "}].SolverResults[0].Pres"))

    result = math.pow(Calc_US_Pressure - Measured_US_Pressure, 2) + math.pow(Calc_DS_Pressure - Measured_DS_Pressure, 2)
     
    print(result)
    return result

def TuneManifoldPressureDrop(inputManifoldData):

    inputManifoldPressureData = inputManifoldData[inputManifoldData["Property"] == "Flowline Pressure"]
    Flowlines = list(np.unique(inputManifoldPressureData["Flowline"].values))

    for flowline in Flowlines:
        manifoldData = inputManifoldPressureData[inputManifoldPressureData["Flowline"] == flowline]

        global Measured_US_Pressure
        global Measured_DS_Pressure
        global US_Joint
        global DS_Joint
        global pipes
        pipes = []
        Measured_US_Pressure = manifoldData["Value"].values[0]
        Measured_DS_Pressure = manifoldData["Value"].values[-1]
        US_Joint = manifoldData["Joint"].values[0]
        DS_Joint = manifoldData["Joint"].values[-1]
        main_flowline = flowline
        if Measured_US_Pressure > 0 and Measured_DS_Pressure > 0:
            for index, row in manifoldData.iterrows():
                temp_pressure = row["Value"]
                pipes.append(row["Pipe"])
                if temp_pressure > Measured_US_Pressure:
                    Measured_DS_Pressure = Measured_US_Pressure
                    Measured_US_Pressure = temp_pressure
                    DS_Joint = US_Joint
                    US_Joint = row["Joint"]

                #check if flowline is commingled with other flowlines
                temp_commingled_flow = row["Commingled Flowline"]
                temp_flowline = row["Flowline"]

                if temp_commingled_flow != temp_flowline:
                    main_flowline = temp_commingled_flow

            #get joints to mask and unmask
            joints_unmask = list(inputManifoldPressureData[inputManifoldPressureData["Commingled Flowline"] == main_flowline]["Joint"].values)
            joints_mask = list(inputManifoldPressureData[inputManifoldPressureData["Commingled Flowline"] != main_flowline]["Joint"].values)

            MaskGAPJoint(joints_mask)
            UnMaskGAPJoint(joints_unmask)

            pipes = list(np.unique(np.array(pipes)))
            gravityCoef = float(petex.DoGet("GAP.MOD[{PROD}].PIPE[{"  + pipes[0] + "}].Matching.AVALS[{Hydro2P}][0]"))
            frictionCoef = float(petex.DoGet("GAP.MOD[{PROD}].PIPE[{" + pipes[0] + "}].Matching.AVALS[{Hydro2P}][1]"))
            bounds =[(0.8, 1.1),(0.3, 3.0)]
            res1 = minimize(ObjectiveFunctionPipe, x0 = (gravityCoef, frictionCoef), method= 'L-BFGS-B', bounds = bounds, options={'ftol' : 0.05, 'eps' : 0.01, 'maxfun' : 1000, 'maxiter' : 100, 'verbose': 1, 'disp': True})
                
            for pipe in pipes:
                petex.DoSet("GAP.MOD[{PROD}].PIPE[{" + pipe + "}].Matching.AVALS[{Hydro2P}][0]", res1.x[0])
                petex.DoSet("GAP.MOD[{PROD}].PIPE[{" + pipe + "}].Matching.AVALS[{Hydro2P}][1]", res1.x[1])

    #unmask all
    joints_unmask = list(inputManifoldPressureData["Joint"].values)
    UnMaskGAPJoint(joints_unmask)

def GetCalculatedOutput(inputWellData, OutputToExcel = True, data_dir = None, fileString = None):
    
    petex.DoGAPFunc('GAP.SOLVENETWORK(0)') #Solve network again just in case 
    wellNames = list(inputWellData["WellName"].values)
    #result
    WHPs = []
    BHPs = []
    OilRates = []
    WaterRates = []
    GasRates = []
    GasLiftRates  = []
    
    for count, well in enumerate(wellNames):
        wellstring = well[0:3] + '_' + well[3:]

        oilRate = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].OilRate"))
        waterRate = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].WatRate"))
        gasRate = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].GasRate"))
        FWHP = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].Pres"))
        FBHP = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].GaugePressure[0]"))
        gasLiftRate = float(petex.DoGet("GAP.MOD[{PROD}].WELL[{" + wellstring + "}].SolverResults[0].Qgin"))

        WHPs.append(FWHP)
        BHPs.append(FBHP)
        OilRates.append(oilRate)
        WaterRates.append(waterRate)
        GasRates.append(gasRate)
        GasLiftRates.append(gasLiftRate)

    Result = {'WellName' : wellNames, 'FWHP' : WHPs, 'FBHP' : BHPs, 'OilRate' : OilRates, 
              'WaterRate' : WaterRates, 'GasRate' : GasRates, "GasLiftRate": GasLiftRates}
              
    Columns = ['WellName', 'FWHP', "FBHP", "OilRate", 'WaterRate', 'GasRate', "GasLiftRate"]
    df  = pd.DataFrame(Result, columns = Columns)

    if OutputToExcel:
        if data_dir is None:
            data_dir = os.getcwd()
            if fileString is None:
                fileString = 'result.csv'
            path = os.path.join(data_dir, fileString)
            df.to_csv(path, header = True)

    return df

def OptimizeGapModel(inputWellData, MaxGasConstraint):

    petex.DoGAPFunc('GAP.SOLVENETWORK(0)')
    wellNames = list(inputWellData["WellName"].values)
    petex.DoSet("GAP.MOD[{PROD}].MaxQgas", MaxGasConstraint)

    #Field Wide Choke Optimization
    for well in wellNames:
        string = well[0:3] + "_" + well[3:] + "CK"
        petex.DoSet("GAP.MOD[{PROD}].INLCHK[{" + string + "}].DPControl", "CALCULATED")
        petex.DoSet("GAP.MOD[{PROD}].INLCHK[{" + string + "}].ChokeDiameterMin", 0.5)
        petex.DoSet("GAP.MOD[{PROD}].INLCHK[{" + string + "}].ChokeDiameterMax", 6.0)

    petex.DoGAPFunc('GAP.SOLVENETWORK(1)')

