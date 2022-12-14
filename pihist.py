"""Python interface to the OSIsoft PI SDK for interacting with a PI historian.

This module has several advantages over using the PI SDK directly:
    1 - Simplicity.  The PI SDK uses a large class hierarchy, whereas this
        module has only two classes (Server and Tag) that the user needs, and
        all return values are Python built-in types (numbers, strings, lists,
        and dictionaries) and Pandas data structures (Series, DataFrames).
    2 - Robustness.  The user may make large requests from a PI historian using
        a single function call, and this module will automatically split the
        request into smaller queries to avoid network timeouts and lost data.
"""
#TODO   why is kr1_1300-161-SDV-3015-4.STS returning NaNs?
#TODO   figure out how to use filter_expressions, document their use

import sys
import math
import win32com.client
import pythoncom
import pywintypes
import datetime
import pytz
import numpy as np
import pandas as pd

sdk = None # single, global interface to the PI SDK
def sdk_init():
    global sdk
    if sdk is None:
        sdk = PISDK() # initialize the PISDK object


class PISDK(object):
    """Wrapper around the PI SDK (not any particular server)."""

    def __init__(self):
        # COM PISDK object
        self._pi_sdk = win32com.client.Dispatch('PISDK.PISDK')

    def get_servers(self):
        """Return the names of all PI historians registered on this computer.
           Any of these may be used as the server_name argument to the Server
           constructor.
        """
        return [s.Name for s in self._pi_sdk.Servers]

    def get_sdk_version(self):
        """Return the version number for the currently installed library. """
        return self._pi_sdk.PISDKVersion.Version

    def get_dll_path(self):
        """Return the full, local file and path name for the PI SDK DLL. """
        return self._pi_sdk.PISDKVersion.Path

    def get_constant(self, collection_name, constant_name):
        """Return an enumerated constant required by an
           internal PI SDK function call.
        """
        collection = self._pi_sdk.PIConstants.Item(collection_name)
        return collection.Item(constant_name)


class Server:
    """Interface to a particular PI historian.

    This class opens a connection to a PI historian upon construction.  Once
    this is complete, the user may query properties of the PI historian itself,
    or may request bulk downloads of PI tag names, attributes, and data from
    the PI historian.  Getting data for a single PI tag might be easier using
    the Tag class.
    """

    def __init__(self, server_name, user_name=None, password=None):
        """Open a connection to a single PI historian.

        Args:
            server_name -- name of the PI historian to connect to
            user_name   -- optional user name to log into the server with;
                           try 'piuser' if the default doesn't work
            password    -- optional password to log into the server with
        """
        sdk_init()

        # Open a connection to the PI historian
        connection_string = ''
        if user_name is not None:
            connection_string += 'UID=' + user_name
        if password is not None:
            connection_string += 'PWD=' + password
        self._pi_server = sdk._pi_sdk.Servers(server_name)
        self._pi_server.Open(connection_string)

        # Build a COM time format object to be cloned for all time I/O
        self._pi_time = win32com.client.Dispatch('PITimeServer.PITimeFormat')
        try:
            tzi = self._pi_server.PITimeZoneInfo
            #self._pi_time.TimeZoneInfo = tzi
            # For some reason, simply setting this parameter fails.
            # This seems to be required instead:
            attr = 'TimeZoneInfo'
            self._pi_time.__LazyMap__(attr)
            entry = self._pi_time._olerepr_.propMapPut[attr]
            self._pi_time._oleobj_.Invoke(entry.dispid, 0,
                                          pythoncom.INVOKE_PROPERTYPUTREF, 0,
                                          tzi)
        except Exception as e:
            print('** Error setting TimeZoneInfo: ' + str(e))
        self._pi_time.FormatString = 'yyyy-MM-dd hh:mm:ss'

    def __del__(self):
        """Close the connection to the PI historian. """
        if self._pi_server is not None:
            self._pi_server.Close()

    def get_timeout(self):
        """Return the current value of the server timeout delay, in seconds."""
        return self._pi_server.Timeout

    def set_timeout(self, timeout_secs=600):
        """Set the current value of the server timeout delay.

        Efficiently downloading large amounts of data from a PI historian might
        depend on finding good settings for both this timeout and for
        the maximum number of values to grab in any one batch.

        Args:
            timeout_secs -- number of seconds to wait for a data request before
                            canceling
        """
        self._pi_server.Timeout = timeout_secs

    def get_server_time(self):
        """Return a string with the local date and time at the
           PI historian.
        """
        self._pi_time.UTCSeconds = self._pi_server.ServerTime().UTCSeconds
        return self._pi_time.OutputString

    def get_server_name(self):
        """Return the name of the PI historian. """
        return self._pi_server.Name

    def get_server_path(self):
        """Return the network path to the PI historian. """
        return self._pi_server.Path

    def get_server_port(self):
        """Return the TCP/IP port number used by the PI historian. """
        return self._pi_server.Port

    def get_server_os(self):
        """Return the operating system used by the PI historian. """
        return (  self._pi_server.ServerVersion.OSName
                + ' version '
                + self._pi_server.ServerVersion.OSVersion)

    def get_server_version(self):
        """Return the PI software version used by the PI historian. """
        return self._pi_server.ServerVersion.Version

    def get_current_user(self):
        """Return the name of the user logged into the PI historian. """
        return self._pi_server.CurrentUser

    def convert_time(self, t):
        """Convert the time t to a PITimeServer.PITimeFormat COMObject.

        This is for internal use only.

        Args:
            t -- Either a string in 'yyyy-mm-dd HH:MM:SS' format,
                        a PyTime object, or
                        a PITimeServer.PITimeFormat COMObject

        Returns:
            A PITimeServer.PITimeFormat COMObject that may be used directly by
            PI SDK calls.  The input time is interpreted as a local time in the
            server's time zone.
        """
        if (   isinstance(t, str)
            or ((sys.version_info[0]==2) and isinstance(t, unicode)) ):
            pi_time = self._pi_time.Clone()
            pi_time.InputString = t
            return pi_time
        elif isinstance(t, pywintypes.TimeType):
            pi_time = self._pi_time.Clone()
            pi_time.InputString = val.Format('%Y-%m-%d %H:%M:%S')
            return pi_time
        else:
            # XXX Assume it is already a PITimeServer.PITimeFormat COMObject
            return t

    def get_tag_names(self, query='tag="*"'):
        """Get the names of all PI tags that match a given query.

        Args:
            query -- A query expression is composed of one or more phrases
                     separated by or.
                     A phrase is composed of one or more basic tests separated
                     by and.
                     A basic test is:
                        attribute-name relational-op value
                     An attribute-name is a PI tag attribute such as 'tag' or
                     'descriptor'.
                     A relational-op must be one of <, <=, >, >=, =, or <>.
                     A value is a number, string, or date, as required.

        Returns:
            A list of the PI tag names (as strings) that fit the query and
            that the current user has permission to see.
        """
        pi_points = self._pi_server.GetPoints(query, None)
        return [p.Name for p in pi_points]

    def get_tag_attributes(self, tag_names):
        """Get all of the attributes (descriptor, engunits, etc.) for one or
           more PI tags.

        Args:
            tag_names -- iterable collection of PI tag names

        Returns:
            A dictionary mapping tag names to sub-dictionaries,
            each of which maps tag attribute names to tag attribute values.
        """
        # This would be much faster if we called self._pi_server.GetPoints()
        # with a query, then did the Tag.get_all_attributes() work here.
        attributes = dict()
        for tag_name in tag_names:
            try:
                tag = Tag(self, tag_name)
                attrs = tag.get_all_attributes()
            except Exception as e:
                print(  '** Error getting attribute for "' + tag_name + ':  '
                      + str(e))
                attrs = dict()
            attributes[tag_name] = attrs
        return attributes

    def get_tag_raw_data(self, tag_names, start_time, end_time,
                         max_samples_per_request=100000,
                         filter_expression=''):
        """Download every value recorded for one or more PI tags between the
           two given times (inclusive).

        The generator function splits the requests up into smaller individual
        downloads, and dynamically reduces the maximum request size if a server
        timeout occurs (so no data is lost).

        Args:
            tag_names               -- an iterable sequence of PI tag names for
                                       which to download data
            start_time              -- a starting time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            end_time                -- an ending time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            max_samples_per_request -- the maximum number of values to download
                                       during a single request from the PI
                                       historian
            filter_expression       --

        Yields:
            A 3-tuple, the first element of which is the tag name, the second
            element of which is the return value from Tag.get_large_raw_data(),
            and the third element of which is the Tag object.  If an
            unrecoverable error occurs, then the second (and maybe the third)
            elements are None.
        """
        # The SDK only allows us to get data one tag at a time
        for tag_name in tag_names:
            tag = None
            try:
                tag = Tag(self, tag_name)
                data = tag.get_large_raw_data(start_time, end_time,
                                              max_samples_per_request,
                                              filter_expression)
                max_samples_per_request = min(max_samples_per_request,
                                              tag._max_samples_per_request)
            except Exception as e:
                print(  '** Error getting data for "' + tag_name + ':  '
                      + str(e))
                data = None
            yield (tag_name, data, tag)

    def get_tag_interpolated_data(self, tag_names, start_time, end_time,
                                  interval_secs=600,
                                  max_samples_per_request=50000,
                                  filter_expression='',
                                  improve_start_time=False):
        """Download interpolated values of one or more PI tags.

        The data is requested at evenly spaced points in time between the two
        given times (inclusive).  The generator function splits the requests up
        into smaller individual downloads, and dynamically reduces the maximum
        request size if a server timeout occurs (so no data is lost).
        Note that "interpolation" is up to the PI historian, and may be
        linearly interpolated or set equal to the last recorded value depending
        on how the PI tag was set up.

        Args:
            tag_names               -- an iterable sequence of PI tag names for
                                       which to download interpolated data
            start_time              -- a starting time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            end_time                -- an ending time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            interval_secs           -- the desired, fixed Delta t, in seconds,
                                       between interpolated points
            max_samples_per_request -- the maximum number of interpolated
                                       values to download during a single
                                       request from the PI historian
            filter_expression       --
            improve_start_time      -- if True, then if start_time is earlier
                                       than the tag's creation date, then reset
                                       start_time to midnight of the tag's
                                       creation date before the download;
                                       midnight is used rather than the exact
                                       creation time because the tag creation
                                       time might not be an "even" number
                                       relative to interval_secs

        Yields:
            A 3-tuple, the first element of which is the tag name, the second
            element of which is the return value from
            Tag.get_large_interpolated_data(), and the third element of which
            is the Tag object.  If an unrecoverable error occurs, then the
            second (and maybe the third) elements are None.
        """
        # The SDK only allows us to get data one tag at a time
        for tag_name in tag_names:
            tag = None
            try:
                tag = Tag(self, tag_name)
                data = tag.get_large_interpolated_data(start_time, end_time,
                                                       interval_secs,
                                                       max_samples_per_request,
                                                       filter_expression,
                                                       improve_start_time)
                max_samples_per_request = min(max_samples_per_request,
                                              tag._max_samples_per_request)
            except Exception as e:
                print(  '** Error getting data for "' + tag_name + ':  '
                      + str(e))
                data = None
            yield (tag_name, data, tag)

    def get_tag_time_averaged_data(self, tag_names, start_time, end_time,
                                   interval_secs=600,
                                   max_samples_per_request=50000,
                                   improve_start_time=False):
        """Download time-averaged values of one or more PI tags.

        The data is requested at evenly spaced points in time between the two
        given times (inclusive).  The generator function splits the requests up
        into smaller individual downloads, and dynamically reduces the maximum
        request size if a server timeout occurs (so no data is lost).
        Note that averaging is done by the PI historian assuming that we have
        a true continuous signal, where interpolation between recorded values
        is performed as by get_tag_interpolated_data().

        Args:
            tag_names               -- an iterable sequence of PI tag names for
                                       which to download time-averaged data
            start_time              -- a starting time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            end_time                -- an ending time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            interval_secs           -- the desired, fixed width, in seconds,
                                       of each time interval over which to
                                       average the data
            max_samples_per_request -- the maximum number of intervals to
                                       to download data for during a single
                                       request from the PI historian
            improve_start_time      -- if True, then if start_time is earlier
                                       than the tag's creation date, then reset
                                       start_time to midnight of the tag's
                                       creation date before the download;
                                       midnight is used rather than the exact
                                       creation time because the tag creation
                                       time might not be an "even" number
                                       relative to interval_secs

        Yields:
            A 3-tuple, the first element of which is the tag name, the second
            element of which is the return value from
            Tag.get_large_time_averaged_data(), and the third element of which
            is the Tag object.  If an unrecoverable error occurs, then the
            second (and maybe the third) elements are None.
        """
        # The SDK only allows us to get data one tag at a time
        for tag_name in tag_names:
            tag = None
            try:
                tag = Tag(self, tag_name)
                data = tag.get_large_time_averaged_data(start_time, end_time,
                                                        interval_secs,
                                                        max_samples_per_request,
                                                        improve_start_time)
                max_samples_per_request = min(max_samples_per_request,
                                              tag._max_samples_per_request)
            except Exception as e:
                print(  '** Error getting data for "' + tag_name + ':  '
                      + str(e))
                data = None
            yield (tag_name, data, tag)


class Tag:
    """Interface to a particular PI tag/PI point on a single PI historian.

    The user must have already connection to a PI historian before constructing
    a Tag.  Once constructed, the Tag object may be used to request attributes
    and data from the PI historian for this tag.
    """

    def __init__(self, server, tag_name):
        """Construct a Tag object.

        Args:
            server   -- the existing pihist.Server object that "owns" this tag
            tag_name -- the (string) name of the PI tag of interest
        """
        self._tag_name = tag_name
        self._server   = server

        # The maximum number of values to ask for at once from the server;
        # this may get automatically adjusted to avoid server timeouts, but
        # start by not assuming there are any limits beyond a signed 32-bit int
        self._max_samples_per_request = np.int32(2**31 - 1)

        # Copy the server's PITimeFormat object so that we can manipulate times
        # in the server's time zone
        self._pi_time  = server._pi_time.Clone()

        # This is the PIPoint object through which all server requests are made
        self._pi_point = server._pi_server.PIPoints.Item(tag_name)

        # Choose a default value to return when an error occurs while
        # communicating with the PI historian
        try:
            pt = self._pi_point.PointType
            if (  (pt == sdk.get_constant('PointTypeConstants', 'String').Value)
                | (pt == sdk.get_constant('PointTypeConstants', 'Blob').Value)
                | (pt == sdk.get_constant('PointTypeConstants', 'TimeStamp').Value)):
                # "Blob" means an arbitrary array of binary data.
                # "Digital" means that a data value is a code into the
                #   "digitalset" strings (e.g., 0 for 'Open', 1 for 'Closed').
                # "TimeStamp" data will be converted to strings.
                self._default_value = ''
            else:
                self._default_value = np.NaN
        except Exception:
            # don't worry too much about defaults if it has no pointtype
            self._default_value = None


    def simplify_attribute_value(self, val):
        """Convert an attribute value into a basic Python type.
           This is just a helper function for the other *Attribute*() methods.
        """
        if isinstance(val, pywintypes.TimeType):
            return val.Format('%Y-%m-%d %H:%M:%S')
        elif isinstance(val, win32com.client.CDispatch):
            return str(val)
        return val


    def get_attribute(self, attribute_name):
        """Get a single attribute (descriptor, engunits, etc.) of the
           PI tag.
        """
        val = self._pi_point.PointAttributes.Item(attribute_name).Value
        return self.simplify_attribute_value(val)


    def get_all_attributes(self):
        """Get all of the attributes for the PI tag.

        Returns:
            A dictionary mapping tag attribute names to tag attribute values.
        """
        # The combination of requesting Count and then using GetAttributes()
        # seems to be the best way to force the SDK to grab all of the
        # attributes in a single network request and NOT return a severely
        # reduced set of results.
        n = self._pi_point.PointAttributes.Count
        return {a.Name:self.simplify_attribute_value(a.Value)
                for a in self._pi_point.PointAttributes.GetAttributes()}


    def get_code_names(self):
        """Get the string equivalents of the digital codes for this tag
           (e.g., ['Open','Closed'] for a valve where the interpolated values
           would all be 0 or 1)
        """
        if (   self._pi_point.PointType
            == sdk.get_constant('PointTypeConstants', 'Digital').Value):
            names = self.get_attribute('digitalset').split('_')
        else:
            names = None
        return names


    def pivals_to_series(self, pi_vals):
        """Convert a PIValues Collection into a Pandas Series.
           Iterating over the PIValues collection is very slow.  This function
           encapsulates all of the steps necessary to do this efficiently.

        Returns:
            A tuple:
              1 - the first value of which is the full Pandas Series with all
                  of the resulting data;
              2 - the second value of which is a Boolean list of the same
                  length as the Series with true iff the corresponding element
                  of the Series was marked IsGood by the PI historian;
              3 - the 
        """
        if pi_vals.Count == 0:
            last_time = np.NaN
            is_good = []
            index = None
            data = None
        else:
            sa = pi_vals.GetValueArrays() # "parallel SafeArrays"
            last_time = sa[1][-1]
            is_good = [(sa[2][i] >= 0) for i in range(len(sa[0]))]
            index = [datetime.datetime.fromtimestamp(t, pytz.utc)
                     for t in sa[1]]
            if (   self._pi_point.PointType
                != sdk.get_constant('PointTypeConstants', 'Digital').Value):
                # Having the if statement inside the list comprehension is
                # required to ensure that no PyIDispatch objects are returned
                data = [(sa[0][i] if is_good[i] else self._default_value)
                        for i in range(len(sa[0]))]
            else:
                # In this case, sa[0] contains PyIDispatch objects;
                # retrieving the code from these objects is still much faster
                # than iterating over the pi_vals collection;
                # to convert these codes into strings, index into the
                # "digitalset" attribute
                dispid = sa[0][0].GetIDsOfNames('Code')
                flags = pythoncom.DISPATCH_PROPERTYGET
                data = [(sa[0][i].Invoke(dispid, 0, flags, True)
                         if is_good[i] else self._default_value)
                        for i in range(len(sa[0]))]
        output = pd.Series(data=data, index=index, name=self._tag_name)
        return (output, is_good, last_time)


    def get_raw_data(self, start_time, end_time, filter_expression=''):
        """Download every value recorded for the given tag between the two
           given times (inclusive).  Make only a single request from the PI
           historian, i.e. do not catch any exceptions or automatically break
           the request into smaller requests to avoid server timeouts.

        Args:
            start_time        -- a starting time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            end_time          -- an ending time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            filter_expression --

        Returns:
            All of the values marked as "good" in a single Pandas time Series.
            The times will be monotonic but not necessarily evenly spaced.
            They will all be in Coordinated Universal Time (UTC).
        """
        # Download the data
        pi_vals = self._pi_point.Data.RecordedValues(
            self._server.convert_time(start_time),
            self._server.convert_time(end_time),
            sdk.get_constant('BoundaryTypeConstants', 'Inside'),
            filter_expression,
            sdk.get_constant('FilteredViewConstants', 'Remove Filtered'),
            None)

        # Convert the data from PI SDK types to Python types, filtering out the
        # values not marked as "good"
        (output, is_good, last_time) = self.pivals_to_series(pi_vals)
        output = output[is_good]
        return output


    def get_large_raw_data(self, start_time, end_time,
                           max_samples_per_request=100000,
                           filter_expression=''):
        """This is just like get_raw_data(), except that the processing is
           split across multiple downloads of max_samples_per_request values
           each.  If a server timeout occurs, then max_samples_per_request is
           reduced and the request is re-tried (so no data will be lost).
        """
        t1 = self._server.convert_time(start_time)
        t2 = self._server.convert_time(end_time)
        if t1.UTCSeconds < t2.UTCSeconds:
            dir_const = sdk.get_constant('DirectionConstants', 'Forward')
            dir_delta = +1e-3
        else:
            dir_const = sdk.get_constant('DirectionConstants', 'Reverse')
            dir_delta = -1e-3
        max_samples_per_request = np.int32(min(max_samples_per_request,
                                               self._max_samples_per_request))
        output = pd.Series(name=self._tag_name)
        done = False
        while not done:
            # Download this batch of samples
            try:
                pi_vals = self._pi_point.Data.RecordedValuesByCount(
                    t1, max_samples_per_request, dir_const,
                    sdk.get_constant('BoundaryTypeConstants', 'Inside'),
                    filter_expression,
                    sdk.get_constant('FilteredViewConstants',
                                     'Remove Filtered'),
                    None)
            except Exception as e:
                if is_timeout(e) and (max_samples_per_request > 100):
                    max_samples_per_request = np.int32(
                        max_samples_per_request / 2)
                    print(  '\t*** WARNING:  ' + str(e) + '\n'
                          + '\t***   reducing max_samples_per_request to '
                          + str(max_samples_per_request) + '\n')
                    self._max_samples_per_request = max_samples_per_request
                    continue
                else:
                    print('\t*** ERROR:  ' + str(e))
                    raise

            # Convert the data from PI SDK types to Python types, filtering out
            # the values not marked as "good"
            if pi_vals.Count == 0:
                break
            (tmp,is_good,last_time) = self.pivals_to_series(pi_vals)
            time_limit = datetime.datetime.fromtimestamp(t2.UTCSeconds,
                                                         pytz.utc)
            tmp = tmp[is_good]
            if dir_delta > 0:
                done = last_time >= t2.UTCSeconds
                if done:
                    tmp = tmp[tmp.index < time_limit]
            else:
                done = last_time <= t2.UTCSeconds
                if done:
                    tmp = tmp[tmp.index > time_limit]
            output = pd.concat([output,tmp], axis=0)

            # Move to the next batch unless we've completed the time interval
            t1.UTCSeconds = last_time + dir_delta

        # Return the results
        return output


    def get_interpolated_data(self, start_time, end_time, num_samples,
                              filter_expression=''):
        """Download a fixed number of interpolated values of the given PI tag
           between the two given times (exclusive of the end_time).  Make only
           a single request from the PI historian, i.e. do not catch any
           exceptions or automatically break the request into smaller requests
           to avoid server timeouts.  Note that "interpolation" is up to the PI
           historian, and may be linearly interpolated or set equal to the last
           recorded value depending on how the PI tag was set up.

        Args:
            start_time        -- a starting time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            end_time          -- an ending time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            num_samples       -- the number of samples to download
            filter_expression --

        Returns:
            All of the values in a single Pandas time Series, with the values
            not marked as "good" replaced by the default value for this tag.
            The times will be monotonic and equally spaced.
            They will all be in Coordinated Universal Time (UTC).
        """
        # Download the data; note that InterpolatedValues() always returns the
        # value at end_time, which we don't want, so we ask for one extra
        # sample and drop it later
        pi_vals = self._pi_point.Data.InterpolatedValues(
            self._server.convert_time(start_time),
            self._server.convert_time(end_time),
            num_samples + 1,
            filter_expression,
            sdk.get_constant('FilteredViewConstants', 'Remove Filtered'),
            None)

        # Convert the data from PI SDK types to Python types
        (output, is_good, last_time) = self.pivals_to_series(pi_vals)
        output = output.iloc[0:-1] # delete the extra sample at end_time
        return output


    def get_large_interpolated_data(self, start_time, end_time,
                                    interval_secs=600,
                                    max_samples_per_request=50000,
                                    filter_expression='',
                                    improve_start_time=False):
        """This is just like get_interpolated_data(), except that a specific
           Delta t is imposed and the processing is split across multiple
           downloads.  If a server timeout occurs, then the download size is
           reduced and the request is re-tried (so no data will be lost).

        Args:
            start_time              -- a starting time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            end_time                -- an ending time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            interval_secs           -- the time difference (in seconds) between
                                       samples
            max_samples_per_request -- the maximum number of samples to request
                                       at a time; if a server timeout occurs,
                                       then this will be reduced and stored in
                                       self._max_samples_per_request for the
                                       caller's reference
            filter_expression       --
            improve_start_time      -- if True, then if start_time is earlier
                                       than the tag's creation date, then reset
                                       start_time to midnight of the tag's
                                       creation date before the download;
                                       midnight is used rather than the exact
                                       creation time because the tag creation
                                       time might not be an "even" number
                                       relative to interval_secs

        Returns:
            All of the values in a single Pandas time Series, with the values
            not marked as "good" replaced by the default value for this tag.
            The times will be monotonic and equally spaced.
            They will all be in Coordinated Universal Time (UTC).
        """

        # Get the request parameters
        t1 = self._server.convert_time(start_time)
        if improve_start_time:
            try:
                cd_str = (  tag.get_attribute('creationdate').split()[0]
                          + ' 00:00:00')
                self._pi_time.InputString = cd_str
                t1.UTCSeconds = max(t1.UTCSeconds,
                                    self._pi_time.UTCSeconds)
            except Exception:
                pass # just use the input start_time if getting the
                     # PI tag's creationdate fails for some reason
        t2 = self._server.convert_time(end_time)
        t_end = t2.Clone()
        max_samples_per_request = np.int32(min(max_samples_per_request,
                                               self._max_samples_per_request))

        # Download the data
        output = pd.Series(name=self._tag_name)
        while True:
            # Decide how many samples to grab in this batch.
            # Include the end time of the last batch, or the start time of
            # this batch if this is the first batch
            num_complete_intervals = math.floor(
                1.0 * (t_end.UTCSeconds - t1.UTCSeconds) / interval_secs)
            num_samples = int(min(
                max_samples_per_request, num_complete_intervals))
            if num_samples <= 0:
                break

            # Download this batch of samples
            t2.UTCSeconds = t1.UTCSeconds + num_samples*interval_secs
            try:
                new_output = self.get_interpolated_data(
                    t1, t2, num_samples, filter_expression)
            except Exception as e:
                if is_timeout(e) and (num_samples > 100):
                    max_samples_per_request = np.int32(num_samples / 2)
                    print(  '\t*** WARNING:  ' + str(e) + '\n'
                          + '\t***   reducing max_samples_per_request to '
                          + str(max_samples_per_request) + '\n')
                    self._max_samples_per_request = max_samples_per_request
                    continue
                else:
                    print('\t*** ERROR:  ' + str(e))
                    raise
            output = pd.concat([output,new_output], axis=0)

            # Go to the next batch
            t1.UTCSeconds = t2.UTCSeconds

        # Return the results
        return output


    def get_time_averaged_data(self, start_time, end_time, num_intervals):
        """Download a fixed number of time-averaged values of the given PI tag
           between the two given times (exclusive of the end_time).  Make only
           a single request from the PI historian, i.e. do not catch any
           exceptions or automatically break the request into smaller requests
           to avoid server timeouts.  Note that averaging is done by the PI
           historian assuming that we have a true continuous signal, where
           interpolation between recorded values is performed as by
           get_interpolated_data().

        Args:
            start_time        -- a starting time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            end_time          -- an ending time (in the server's time zone) in
                                 one of the formats accepted by
                                 Server.convert_time()
            num_intervals     -- the number of samples to download
            filter_expression --

        Returns:
            A Pandas DataFrame, indexed by time.  The times will be monotonic
            and equally spaced.  They will all be in Coordinated Universal Time
            (UTC).  For each interval:
                index -- time at the start of the interval
                Avg   -- time-averaged value of the tag over the interval
                Std   -- time-averaged standard deviation of the tag over the
                         interval
                Min   -- minimum value recorded for the tag over the interval
                Max   -- maximum value recorded for the tag over the interval
                Num   -- number of recorded events for the tag over the
                         interval; note that Num==0 does not imply that the
                         data is bad
            We used to include the percentage of the raw data that was "good"
            (via cnt_vals[n].ValueAttributes.Item('PercentGood').Value), but
            this would add a significant amount of COM overhead.
        """
        # Download the data
        pi_summaries = self._pi_point.Data.Summaries(
            self._server.convert_time(start_time),
            self._server.convert_time(end_time),
            sdk.get_constant('BoundaryTypeConstants', 'Inside'),
            sdk.get_constant('ArchiveSummariesTypeConstants',
                             'All Supported Summary types'),
            num_intervals,
            sdk.get_constant('CalculationBasisConstants', 'Time Weighted'),
            None)
        pi_counts = self._pi_point.Data.Summaries(
            self._server.convert_time(start_time),
            self._server.convert_time(end_time),
            sdk.get_constant('BoundaryTypeConstants', 'Inside'),
            sdk.get_constant('ArchiveSummariesTypeConstants', 'Count'),
            num_intervals,
            sdk.get_constant('CalculationBasisConstants', 'Event Weighted'),
            None)

        # Convert the data from PI SDK types to Python types
        if pi_summaries.Count == 0:
            output = pd.DataFrame(columns=['Avg', 'Std', 'Min', 'Max', 'Num'])
        else:
            (avg_vals, is_good, last_time) = self.pivals_to_series(pi_summaries.Item('Average').Value)
            (std_vals, is_good, last_time) = self.pivals_to_series(pi_summaries.Item('StdDev' ).Value)
            (min_vals, is_good, last_time) = self.pivals_to_series(pi_summaries.Item('Minimum').Value)
            (max_vals, is_good, last_time) = self.pivals_to_series(pi_summaries.Item('Maximum').Value)
            (cnt_vals, is_good, last_time) = self.pivals_to_series(pi_counts.Item('Count').Value)
            output = pd.concat([avg_vals, std_vals, min_vals, max_vals, cnt_vals],
                               axis=1)
            output.columns=['Avg', 'Std', 'Min', 'Max', 'Num']
            output.Num = output.Num.astype(int)
        return output


    def get_large_time_averaged_data(self, start_time, end_time,
                                     interval_secs=600,
                                     max_samples_per_request=50000,
                                     improve_start_time=False):
        """This is just like get_time_averaged_data(), except that a specific
           Delta t is imposed and the processing is split across multiple
           downloads.  If a server timeout occurs, then the download size is
           reduced and the request is re-tried (so no data will be lost).

        Args:
            start_time              -- a starting time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            end_time                -- an ending time (in the server's time
                                       zone) in one of the formats accepted by
                                       Server.convert_time()
            interval_secs           -- the time difference (in seconds) between
                                       samples
            max_samples_per_request -- the maximum number of samples to request
                                       at a time; if a server timeout occurs,
                                       then this will be reduced and stored in
                                       self._max_samples_per_request for the
                                       caller's reference
            improve_start_time      -- if True, then if start_time is earlier
                                       than the tag's creation date, then reset
                                       start_time to midnight of the tag's
                                       creation date before the download;
                                       midnight is used rather than the exact
                                       creation time because the tag creation
                                       time might not be an "even" number
                                       relative to interval_secs

        Returns:
            same as get_time_averaged_data()
        """

        # Get the request parameters
        t1 = self._server.convert_time(start_time)
        if improve_start_time:
            try:
                cd_str = (  tag.get_attribute('creationdate').split()[0]
                          + ' 00:00:00')
                self._pi_time.InputString = cd_str
                t1.UTCSeconds = max(t1.UTCSeconds,
                                    self._pi_time.UTCSeconds)
            except Exception:
                pass # just use the input start_time if getting the
                     # PI tag's creationdate fails for some reason
        t2 = self._server.convert_time(end_time)
        t_end = t2.Clone()
        max_samples_per_request = np.int32(min(max_samples_per_request,
                                               self._max_samples_per_request))

        # Download the data
        output = pd.DataFrame()
        while True:
            # Decide how many samples to grab in this batch.
            num_complete_intervals = math.floor(
                1.0 * (t_end.UTCSeconds - t1.UTCSeconds) / interval_secs)
            num_intervals = int(min(
                max_samples_per_request, num_complete_intervals))
            if num_intervals <= 0:
                break

            # Download this batch of samples
            t2.UTCSeconds = t1.UTCSeconds + num_intervals*interval_secs
            try:
                new_output = self.get_time_averaged_data(t1, t2, num_intervals)
            except Exception as e:
                if is_timeout(e) and (num_intervals > 100):
                    max_samples_per_request = np.int32(num_intervals / 2)
                    print(  '\t*** WARNING:  ' + str(e) + '\n'
                          + '\t***   reducing max_samples_per_request to '
                          + str(max_samples_per_request) + '\n')
                    self._max_samples_per_request = max_samples_per_request
                    continue
                else:
                    print('\t*** ERROR:  ' + str(e))
                    raise
            output = pd.concat([output,new_output], axis=0)

            # Go to the next batch
            t1.UTCSeconds = t2.UTCSeconds

        # Return the results
        return output


def is_timeout(e):
    """Determine if e is a server timeout exception. """
    return (    isinstance(e, pywintypes.com_error)
            and isinstance(e.excepinfo, tuple)
            and (len(e.excepinfo) >= 3)
            and (e.excepinfo[2].lower().find('timeout') >= 0) )


def write_tag_attributes(fp, tag_attributes,
                         attrs=['tag', 'descriptor', 'typicalvalue',
                                'engunits', 'exdesc', 'instrumenttag',
                                'sourcetag', 'pointsource', 'pointtype',
                                'totalcode', 'digitalset', 'compressing',
                                'creationdate', 'changedate']):
    """Output a set of tag attributes to a file. """
    def print_line(values):
        line = u'\t'.join(values) + u'\n'
        fp.write(line.encode('utf-8'))

    print_line(attrs)
    for tag in sorted(tag_attributes):
        print_line([tag_attributes[tag].get(a,'') for a in attrs])


def convert_string_to_utc(server, s):
    """ Convert a single date/time string from the server's time zone to UTC """
    t = server.convert_time(s).UTCSeconds
    dt = datetime.datetime.fromtimestamp(t, pytz.utc)
    return dt


def convert_strings_to_utc(server, tag_data):
    """ Convert date/time strings from the server's time zone to UTC """
    dts = [convert_string_to_utc(server, s) for s in tag_data.values]
    return pd.Series(data=dts, index=tag_data.index, name=tag_data.name)

