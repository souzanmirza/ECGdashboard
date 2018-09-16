import wfdb
import csv
import json
import numpy as np

def saverecord(savedir, recorddir, recordname):
    signals, fields = wfdb.rdsamp(recordname, pb_dir=recorddir)
    outfileprefix = savedir + '/' + recordname
    savetocsv(outfileprefix, signals)
    metadata(outfileprefix, fields)

def savetocsv(outfileprefix, signals):
    np.savetxt(outfileprefix+'_signals', signals, fmt='%.32f')
    
def savepatientdatamghdbtojson(comments):
    comment0 = comments[0]
    commentsdict = {}
    if comment0.find('<')==-1:
        print('Bad header')
        return commentsdict
    print(comment0)
    comment0=comment0.replace('<','*')
    comment0 = comment0.replace('>:', '*')
    comment0 = comment0.split('*')
    print(comment0, len(comment0))
    for i in [1, 3, 5]:
        commentsdict[comment0[i]] = comment0[i+1].replace(' ', '')
    return commentsdict

def metadata(outfileprefix, fields):
    commentsdict = savepatientdatamghdbtojson(fields['comments'])
    fields=dict((k, fields[k]) for k in fields.keys() if k!='comments')
    newfields = dict(list(fields.items()) + list(commentsdict.items()))
    with open(outfileprefix + '_metadata.txt', 'w') as metadataoutfile:
        json.dump(newfields, metadataoutfile, indent=4, sort_keys=True)

def main():
    directory = "/home/souzan/Documents/data/mghdb"
    recordsfile = open(directory + "/RECORDS.txt", 'r')
    records = recordsfile.readlines()
    recorddir = 'mghdb'
    for i in [0]:
        recordname = records[i].replace('\n', '')
        saverecord(directory, recorddir, recordname)

if __name__ == '__main__':
    main()