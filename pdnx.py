# When NXclassic_scan in use: change to entry = None, data = None in pdnx.__init__

import nexusformat.nexus as nx
import pandas as pd
import matplotlib
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from IPython.display import display, HTML

pd.set_option('display.max_rows',8)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width',999)

#scandata_field_list = ['/entry1/measurement', '/entry1/plotted']
#scan_command_field_list = ['/entry1/scan_command']
_entry = '/entry1'
_measurement = '/measurement'
_scannables_node = '/before_scan'



class pdnx(pd.DataFrame): 
    '''
    nexusformat wrapper: tries to create dataframe from default data
    whole nexus file is under .nx attribute 
    either: specify entry and data field (data field must contain only data of the same length)
    or use NXclassic_scan definition (looks only for first entry or subentry in first entry)
        (the latter converts only fields used by scannables in scan and preserves order) 
    e.g. 
    n=pdnx(p % 633777)  open file for scan 633777 (p is filename/format specifier)
    n                   display pandas dataframe
    n.plot()            plot pandas dataframe
    n.plot('idgap','ic1monitor')	pandas plot with selected x and y collumns
    n.plt('idgap','ic1monitor')		same but with pdnx defaults (title etc)	
    n.nx                nexus tree
    print(n.nx.tree)     print nexus tree
    n.find('chi')	find 'chi' key(s) in tree and display value(s) (n.find() for all)
    n.findkeys('chi')	return list of key value lists for key 'chi'
    n.pruned_tree(n)    return nexus tree up to n levels deep
    n.nx.plot()         default nexus plot
    for i in range(633777, 633779):print(pdnx(p % i).scan)     print scan string for range of scans

    n['newkey'] = n.nx.entry1.before_scan.myval 	as long as 'newkey' is new then this pads out a new scan column with myval
    n.to_excel(filename)    save excel spreadsheet (standard Pandas method - see other .to_ methods)
    n.to_srs(filename)       save as SRS .dat file (requires NXclassic_scan)
    n.to_srs_plus(filename)    save as SRS .dat file with key-value metadata assignments (requires NXclassic_scan)

    '''


    def __init__(self,  filestr, entry = _entry, data = _measurement, round = True):
        '''
        entry = select nexus entry for measurement data and set default to this entry
        data = nexus field containing datafor pandas dataframe
        round: Attempt to round data using @decimals attributes if they exist
        '''
              
        try:
            _nx = nx.nxload(filestr,'r')

        except:
            print("=== Error loading NeXus file %s" % filestr)
            return
        
        _load_dataframe_success = False
        _use_classicscan = False

        entrydata = None
        # use specified entry and NXdata if given
        if not entry == None and not data == None:
            entrydata = entry+data
        else:
            # try to use NXclassic_scan to get (sub)entry and NXdata
            try:
                entry =  getNexusSubentryWithDefinition(_nx, definition = 'NXclassic_scan')
                for key in _nx[entry].keys():
                    if 'NXdata' in str(type(_nx[entry][key])):
                        entrydata = '%s/%s' % (entry, key)
                        _use_classicscan = True
            except:
                pass
                        


        try:
            if _use_classicscan:
                #keys = _nx[entrydata]['scan_fields'] # use fields from scan_fields required by NXclassic_scan
                keys = _nx[entry]['scan_fields'] # use fields from scan_fields required by NXclassic_scan

            else:
                keys = _nx[entrydata].keys()        # use all fields - must all be the same length to avoid an error

            nx_scan_dict = {}

            # loop through scan field keys
            for key in keys:
                try:
                    # convert scan data to columns
                    nx_scan_dict[key] = _nx[entrydata][key].nxdata.flatten()
                    if round == True:
                        try: # try to round
                            decimals = _nx[entrydata][key].attrs['decimals']
                            nx_scan_dict[key] = nx_scan_dict[key].round(decimals)
                            if decimals == 0:
                                nx_scan_dict[key] = nx_scan_dict[key].astype(int)   #convert to int if no decimals
                        except:
                            pass
                except:
                    pass
            # create dataframe
            pd.DataFrame.__init__(self, nx_scan_dict, columns = keys)
    
            _load_dataframe_success = True
        except:
            pass

        try:
            _nx['default'] = entry #set default entry to specified entry (for files with multiple enties)
        except:
            pass

        if not _load_dataframe_success:
            print('=== Failed to create DataFrame from data - create empty DataFrame')
            pd.DataFrame.__init__(self)

        setattr(self,'nx',_nx)  # causes a warning in pandas - suppress warnings to avoid messages

        try:
            setattr(self, 'scan', filestr+'\n' + str(_nx[entry]['title'].nxdata))
        except:
            pass

        self._use_classicscan = _use_classicscan
        self._entrydata = entrydata
        self._entry = entry


    def to_srs(self, outfile, extra_metadata = []):
        #save data in SRS .dat format (requires NXclassic_scan)
        #prototype looks for named field (scan) - need to modify to find field containing classic_scan definition
        if not self._use_classicscan:
            raise ValueError('=== The to_srs method requires a NeXus file with NXclassic_scan definition. \nYou might still be able to use .to_csv')
        self.to_csv(outfile, sep = '\t', index = False)
        with open(outfile, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            #for headerline in list(self.nx.entry1.scan.scan_header) + extra_metadata:
            for headerline in list(self.nx[self._entry]['scan_header']) + extra_metadata:
                f.write(headerline + '\n')
            f.write(' &END\n')
            f.write(content)



    def to_srs_plus(self, outfile):
        #save data in SRS .dat format with extra metadata key-value pairs (requires NXclassic_scan)
        #prototype looks for named field (scan) - need to modify to find field containing classic_scan definition
        if not self._use_classicscan:
            raise ValueError('=== The to_srs_plus method requires a NeXus file with NXclassic_scan definition. \nYou might still be able to use .to_csv')
        #positioner_tree_list  =  self.nx.entry1.scan.positioners.tree.split('\n')
        positioner_tree_list  =  self.nx[self._entry].positioners.tree.split('\n')
        assignments_list  =  [assig.lstrip() for assig in positioner_tree_list if '=' in assig and not '@'in assig]
        try:
            scan_command_assignment = ["scan_command = '%s'" % str(self.nx[self._entry].scan_command)]
        except:
            scan_command_assignment = []
        assignments_list  =  ['<MetaDataAtStart>'] + scan_command_assignment + assignments_list + ['</MetaDataAtStart>']
        self.to_srs(outfile, assignments_list)



    def meta(self, *args):
        '''
        Display scannable metadata directly under node specified by _scannable_collection
        No inputs: display all scannables
        One or more scannable name strings - display only those scannables    
        '''
        _all_scannables = list(args)
        if _all_scannables == []:
            _all_scannables = self.nx[_entry + _scannables_node].keys()

        _scannables, _fields, _values = [], [], []

        for scannable in _all_scannables:

            for scannable_field in self.nx[_entry + _scannables_node][scannable].keys():

                _scannables += [scannable]
                _fields += [scannable_field]
                _values += [self.nx[_entry + _scannables_node][scannable][scannable_field]]

        #display by converting to dataframe and using HTML display
        _meta_frame = pd.DataFrame(zip(_scannables, _fields, _values), columns = ['Scannable', 'Field', 'Value'], index = None)
        _rows = pd.get_option('display.max_rows')
        # change max rows from default to long (999) and then back again
        pd.get_option('display.max_rows', 999)
        display(HTML(_meta_frame.to_html(index=False)))
        pd.set_option('display.max_rows', _rows)
       
        
        

    def plt(self, *args, **kwargs):
        _title_length = 90
        kwargs.setdefault('title', self.scan[:_title_length])
        kwargs.setdefault('grid', True)
        self.plot(*args, **kwargs)


    def _list_to_dot_sep_string(self, lst):
        outstr = ''
        for item in lst:
            outstr += '.' + str(item)
        return outstr

    def _find_key(self, tree, key, previous_keys=[]):
        global _keylist
        try:
            for keyval in tree.keys():
                if keyval == key or key == '':
                    _keylist += [previous_keys + [keyval]]
                self._find_key(tree[keyval], key, previous_keys = previous_keys + [keyval])
        except:
            pass

    def findkeys(self, keystring):
        'Return list of key sequences (lists) that end with keystring'    
        global _keylist
        _keylist=[]
        self._find_key(self.nx, keystring)
        return _keylist

    def find(self, keystring=''):
        'Return nexus fields and values for keystring'
        for key_sequence in self.findkeys(keystring):
            obj = self.nx
            for key in key_sequence:
                obj = obj[key]
            print('.nx' + self._list_to_dot_sep_string(key_sequence) + ' : \t', obj)

    def pruned_tree(self, depth):
        'Print pruned tree'
        allfieldlist = self.findkeys('')
        previous = []
        for fieldlist in allfieldlist:
            fieldshort = fieldlist[:depth]
            if fieldshort != previous:
                print(self._list_to_dot_sep_string(fieldshort))
                previous = fieldshort


def getNexusSubentryWithDefinition(nxroot, definition = None):
    '''
    return NeXus tree branch string that is an entry or subentry containing the specified definition (string)
    if no definition specified then the function displays all the definitions found
    '''
    field_with_definition = None
    all_definitions = []

    for entry in nxroot.keys():                             # loop through each entry
        try:
            defn = str(nxroot[entry]['definition'])
            all_definitions += [defn]
            if defn == definition:
                #field_with_definition = nxroot[entry]           # if it has required definition then break
                field_with_definition = '/%s' % entry 
                break
        except:
            pass
    
        for subentry in nxroot[entry].keys():               # loop through each field in entry
            if 'NXsubentry' in str(type(nxroot[entry][subentry])): # if it is a subentry ...
                try:
                    defn = str(nxroot[entry][subentry]['definition'])
                    all_definitions += [defn]
                    if defn == definition:  # check if it has the required definition
                        #field_with_definition = nxroot[entry][subentry]
                        field_with_definition = '/%s/%s' % (entry, subentry) 
                        break
                except:
                    pass
                
    if definition == None:
        print(all_definitions)
        
    return field_with_definition


def vec2mat(vecx, vecy, vecz, n_inner=None):
    #matx, maty, matz = vec2mat(vecx, vecy, vecz, n_inner=None)
    #convert vectors from 2D scan to matrices
    #vecx,y,z: arrays (any dimension) or lists
    #matx,y,z: 2D arrays
    #n_inner: Number of points in inner loop - calculated if not specified
    #Arrays are truncated if the size doesn't match the required shape

    vx = np.array(vecx[:]); vy = np.array(vecy[:]); vz = np.array(vecz[:]) #get inputs in standard form
    if n_inner == None:   #calculate number in inner loop by looking for jumps
        jumps = np.abs(np.diff(vx) * np.diff(vy))
        n_inner = matplotlib.mlab.find(jumps>np.mean(jumps))[0] + 1

    n_outer = len(vx) // n_inner
    #reshape matrices
    matx = vx[0:n_inner * n_outer].reshape(n_outer,n_inner)
    maty = vy[0:n_inner * n_outer].reshape(n_outer,n_inner)
    matz = vz[0:n_inner * n_outer].reshape(n_outer,n_inner)

    return matx, maty, matz

