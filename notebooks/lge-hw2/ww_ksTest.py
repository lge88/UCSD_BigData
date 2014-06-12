#!/usr/bin/python
# Li's intermediate data
# Input Format: station:year,[tmin,tmax]
# Output Format: station:meas,[mean values]

import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import mrjob
from sys import stderr
import re,pickle,base64,zlib
import numpy as np
from scipy import stats
import pickle

def NN(vec):
    """nearest neighbor method to substitute missing values in record the nearest left neighbor"""
    # if there is None
    if not None in vec: # None is the only representation of missing value
        return vec
    else:
        for i in xrange(len(vec)):
            if vec[i]==None:
                if i>0: # if vec[i] is not the first item, make it equal to its left neighbor
                    vec[i]=vec[i-1]
                else:  # if vec[i] is the first item, make it equal to the first valid value in this list
                    vec[i]=filter(None,vec)[0]
        return vec

def str2flt(vec):
    """convert string elements to float; missing values are replaced with None"""
    newvec=[]
    for v in vec:
        try:
            newv=float(v)
        except:
            newv=None
        finally:
            newvec.append(newv)
    return newvec

class BasicProtocolJob(MRJob):

    # get input as raw strings
    INPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
    # pass data internally with pickle
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol
    # write output as JSON
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

class ksTest(MRJob):        
    def datasplit_mapper(self,_,line):
        try:
            rec = line.split(",")
            node=rec[0]
            tminLen=int(rec[1])
            tmaxLen=int(rec[2])
            tmin = rec[3:3+tminLen]
            tmin = [np.float(t) for t in tmin]
            tmax = rec[3+tminLen:]
            tmax = [np.float(t) for t in tmax]
#             stderr.write(node+"\t"+str(tmin[:3])+"\t"+str(tmax[:3])+"\n")
            yield "TMIN",(node,tmin)
#             yield ("TMAX",node),tmax
        except Exception, e:
            stderr.write(str(e))

    def ksTest_reducer(self,key,value):
        try:
            meas=key
            vals = list(value)
            rec = {}
            for v in vals:
                rec[v[0]]=v[1]
#                 stderr.write("ksTest\t"+str(v[0])+"\t"+str(v[1][:5])+"\n")
            nodeList=rec.keys()
            for key in nodeList:
                sib = key[:-1]+str(1-int(key[-1]))
                if sib in nodeList:
#                     stderr.write(key+'\t'+sib+'\n')
                    kst,p = stats.ks_2samp(rec[key],rec[sib])
                    if p>0.05:
                        yield (key,sib),p
        except Exception, e:
            stderr.write(str(e))
#     def sortbymeasstn_reducer(self,key,value):
#         try:
#             meas,node=key
#             vals = list(value)
# #             leftChild=
# #             stats.ks_2samp()
#             if len(vals)>30:
#                 yield node,vals
#         except Exception, e:
#             stderr.write(str(e))
    def steps(self):
        return [
            self.mr(mapper=self.datasplit_mapper,
                    reducer=self.ksTest_reducer),
#             self.mr(reducer=self.ksTest_reducer),
#                     reducer=self.ksTest_reducer),
#             self.mr(mapper=self.ksTest_mapper)
        ]

if __name__ == '__main__':
    ksTest.run()