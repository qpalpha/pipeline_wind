# -*- coding: utf-8 -*-
"""
Created on Tue May 28 13:44:11 2019

@author: lixiang5
"""

#%%
import os,sys,re
import numpy as np

#%%
class Ini():
    """
    @author: Lixiang
    
    File example.ini:
    ---------------------------------
    | [main]                        |
    | x = 10                        |
    | y = [ 1, 3, 7, 9 ]            |
    | modelname = newmodel          |
    ---------------------------------
    
    An Ini object can be constructed using a file name that contains Ini entries.
    
    In [1]: from ini import *
    In [2]: ini = Ini("example.ini")
    In [3]: ini.findInt('main~x)
    Out[3]: 10
    In [4]: ini.findString('main~x')
    Out[4]: '10'
    In [5]: ini.findNumVec('main~y')
    Out[5]: array([1., 3., 7., 9.])
    
    Ini object has the following functions:
    - exists()
    - find()
    - findBool()
    - findInt()
    - findNum()
    - findNumVec()
    - findString()
    - findStringVec()
    - keys()
    - setValue()
    
    * Case of characters are ignorable. Users shall put either upper case or lower case into these functions.
    
    """
    def __init__(self,fini):
        self.fini = fini
        self._parser_()
    
#==============================================================================
    @property
    def keys(self):
        return self.DICT.keys
    
    def exists(self,field):
        return field.upper() in self.keys()
    
    def find(self,field):
        return self.DICT[field.upper()]
    
    def findBool(self,field):
        value = self.find(field).lower()
        return bool(value=='true')
    
    def findInt(self,field):
        value = self.find(field)
        return int(value)

    def findIntVec(self,field):
        value = self.find(field)
        return np.array([int(float(v)) for v in value])
        
    def findNum(self,field):
        value = self.find(field)
        return float(value)
 
    def findNumVec(self,field):
        value = self.find(field)
        return np.array([float(v) for v in value])
    
    def findString(self,field):
        return self.find(field)
    
    def findStringVec(self,field):
        return self.find(field)

    def set(self,key,str_value,final=True):
        self.DICT[key] = str_value

#==============================================================================
    PATTERNS = {'include':r'include\s+<[^<>]*ini>',
                'header' :r'\s*[^\=]*\[\w*\]',
                'content':r'\w*\s*\=\s*[^\[\]]+'}
    QUOTE_PATTERN = r'\%[^\%]+\%'
    DICT = dict()
        
    def _parser_(self):
        f = open(self.fini)
        for line in f.readlines():
            line = self._rm_comments(line)
            lcontent = self._rm_blanks(line)
            # If line is not empty
            if lcontent:
                # Check pattern
                pat_type,content = self._get_pattern_type(line.strip())
                if pat_type=='include':
                    pat = re.compile(self.PATTERNS['include'])
                    finis = [self._get_include_ini(ss) for ss in pat.findall(content)]
                    for ff in finis:
                        ini_sub = Ini(ff)
                        ini_sub._parser_()
                        self.DICT.update(ini_sub.DICT)
                elif pat_type=='header':
                    header = self._rm_brackets(self._rm_blanks(content)).upper()
                elif pat_type=='content':
                    content_type = self._get_content_type(content)
                    eq_pos = content.find('=')
                    field = header+'~'*(header!='')+self._rm_blanks(content[:eq_pos].upper())
                    if content_type in ['vec','scala']:
                        self.DICT[field] = self._get_value(content[eq_pos+1:])
                    elif content_type=='start_vec':
                        value_str = content[eq_pos+1:]
                elif pat_type=='muline':
                    value_str = value_str+' '+content
                    if ']' in content:
                        self.DICT[field] = self._get_value(value_str)
        f.close()
        # Substitute %contents%
        for field in self.DICT:
            value = self.DICT[field]
            if type(value) is str:
                new_value = self._sub(value)
            elif type(value) is list:
                new_value = [self._sub(vv) for vv in value]
            self.DICT[field] = new_value
    
    def _get_include_ini(self,s):
        return self._rm_blanks(s.replace('include','').replace('<','').replace('>',''))
    
    def _get_content_type(self,content):
        if '[' in content:
            if ']' not in content: return 'start_vec'
            else: return 'vec'
        else: return 'scala'
        
    def _get_pattern_type(self,string):
        for key in self.PATTERNS:
            pat = re.compile(self.PATTERNS[key])
            content = re.search(pat,string)
            if content is not None:
                return key,content.string
        return 'muline',string
    
    def _rm_comments(self,s):
        return s[:s.find('#')]
    
    def _rm_blanks(self,s):
        return re.sub(r'\s+','',s.strip())
    
    def _rm_brackets(self,s):
        return s.replace('[','').replace(']','')
    
    def _rm_percent(self,s):
        return s.replace('%','').replace('%','')
    
    def _get_value(self,string):
        if ('[' in string) and (']' in string):
            string = self._rm_brackets(string)
            value = [ss for ss in re.split(r'\s',string) if ss]
        else:
            value = self._rm_blanks(string)
        return value
    
    def _sub(self,string):
        pat = re.compile(self.QUOTE_PATTERN)
        search = re.search(pat,string)
        if search is not None:
            sub_old = pat.findall(string)
            sub_new = [self.DICT[self._rm_percent(s).upper()] for s in sub_old]
            for so,sn in zip(sub_old,sub_new):
                string = string.replace(so,sn)
        return string

#%% Cases Transition
def UpperCase(str_list):
    return list(map(lambda x:x.upper(),str_list))
def LowerCase(str_list):
    return list(map(lambda x:x.lower(),str_list))

#%%
if __name__=='__main__':
    ini = Ini('test.ini')
    print('* DICT in ini: {}'.format(ini.DICT))
    print('* Keys in ini: {}'.format(ini.keys()))
    print('* Whether ''fruit'' in ini: {}'.format(ini.exists('fruit')))
    print('* Find ''newline~Test'' in ini: {}'.format(ini.find('newline~Test')))
    print('* Find ''STOCK'' in ini: {}'.format(ini.find('STOCK')))
    print('* FindBool ''logic'' in ini: {}'.format(ini.findBool('logic')))
    print('* FindInt ''myint'' in ini: {}'.format(ini.findInt('myint')))
    print('* FindIntVec ''nums'' in ini: {}'.format(ini.findIntVec('nums')))
    print('* FindNum ''myint'' in ini: {}'.format(ini.findNum('myint')))
    print('* FindNumVec ''nums'' in ini: {}'.format(ini.findNumVec('nums')))
    print('* FindString ''myint'' in ini: {}'.format(ini.findString('myint')))
    print('* FindStringVec ''phone'' in ini: {}'.format(ini.findStringVec('phone')))


