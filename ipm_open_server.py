import win32com.client
import sys
import time

class OpenServer():
    "Class for holding ActiveX reference. Allows license disconnection"
    def __init__(self):
        self.status = "Disconnected"
        self.OSReference = None
        #self.verbose = verbose
    
    def Connect(self):
        self.OSReference = win32com.client.Dispatch("PX32.OpenServer.1")
        self.status = "Connected"
        #if self.verbose:
        print("OpenServer connected")
        
    def Disconnect(self):
        self.OSReference = None
        self.status = "Disconnected"
        #if self.verbose:
        print("OpenServer disconnected")

    def GetAppName(self, sv):
        # function for returning app name from tag string
        pos = sv.find(".")
        if pos < 2:
            sys.exit("GetAppName: Badly formed tag string")
        app_name = sv[:pos]
        if app_name.lower() not in ["prosper", "mbal", "gap", "pvt", "resolve",
                                    "reveal"]:
            sys.exit("GetAppName: Unrecognised application name in tag string")
        return app_name


    def DoCmd(self, cmd):
        #if self.verbose:
        #print('DoCmd:{0}... '.format(cmd), end='')
        # perform a command and check for errors
        lerr = self.OSReference.DoCommand(cmd)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoCmd: " + err)
        #if self.verbose:
        #print('done')

    def DoSet(self, sv, val):
        #if self.verbose:
        #print('DoSet({0}):{1}... '.format(val, sv), end='')

        # set a value and check for errors
        lerr = self.OSReference.SetValue(sv, val)
        app_name = self.GetAppName(sv)
        lerr = self.OSReference.GetLastError(app_name)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoSet: " + err)
        #if self.verbose:
        #print('done')
    
    def DoGet(self, gv):
        #if self.verbose:
        #print('DoGet:{0}... '.format(gv), end='')

        # get a value and check for errors
        get_value = self.OSReference.GetValue(gv)
        app_name = self.GetAppName(gv)
        lerr = self.OSReference.GetLastError(app_name)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoGet: " + err)
        
        #if self.verbose:
        #print('done')

        return get_value


    def DoSlowCmd(self, cmd):
        #if self.verbose:
        #print('DoSlowCmd:{0}... '.format(cmd), end='')

        # perform a command then wait for command to exit and check for errors
        step = 0.001
        app_name = self.GetAppName(cmd)
        lerr = self.OSReference.DoCommandAsync(cmd)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoSlowCmd: " + err)
        while self.OSReference.IsBusy(app_name) > 0:
            if step < 2:
                step = step*2
            # else:
            #     sys.exit()
            time.sleep(step)
        lerr = self.OSReference.GetLastError(app_name)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoSlowCmd: " + err)

        #if self.verbose:
        #print('done')

    def DoGAPFunc(self, gv):
        self.DoSlowCmd(gv)
        DoGAPFunc = self.DoGet("GAP.LASTCMDRET")
        lerr = self.OSReference.GetLastError("GAP")
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("DoGAPFunc: " + err)
        return DoGAPFunc


    def OSOpenFile(self, theModel, appname):
        self.DoSlowCmd(appname + '.OPENFILE ("' + theModel + '")')
        lerr = self.OSReference.GetLastError(appname)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("OSOpenFile: " + err)


    def OSSaveFile(self, theModel, appname):
        self.DoSlowCmd(appname + '.SAVEFILE ("' + theModel + '")')
        lerr = self.OSReference.GetLastError(appname)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("OSSaveFile: " + err)


    def OSCloseFile(self, theModel, appname):
        #self.DoCmd(appname + '.SHUTDOWN ("' + theModel + '")')
        self.DoCmd(appname + '.SHUTDOWN')
        lerr = self.OSReference.GetLastError(appname)
        if lerr > 0:
            err = self.OSReference.GetErrorDescription(lerr)
            self.Disconnect()
            sys.exit("OSCloseFile: " + err)