#############################################################
##  Thanks to https://github.com/tbertinmahieux/MSongsDB.git
#############################################################


import hdf5_getters
from hdf5_utils import *

import sys
import time
import datetime
from tqdm import tqdm

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

t1 = time.time()

schema = avro.schema.parse(open("song.avsc", "rb").read())

writer = DataFileWriter(open("song.avro", "wb"), DatumWriter(), schema, codec='snappy')


maindir = sys.argv[1]

allh5 = get_all_files(maindir, ext='.h5')
print('found',len(allh5),'H5 files.')

entries = schema._props["fields"]

num_of_songs = 0

for h5path in tqdm(allh5):
    h5 = hdf5_getters.open_h5_file_read(h5path)
    
    num_songs = hdf5_getters.get_num_songs(h5)
    num_of_songs += num_songs
    
    for i in range(num_songs):
        song = {}
        for entry in entries:
            name = entry.name
            
            attribute = "get_" + name
            value = hdf5_getters.__getattribute__(attribute)(h5, i)
            
            datatype = entry.type
            value = proper_data_format(datatype, value)
            
            song[name] = value
        writer.append(song)
    h5.close()
    
writer.close()

stimelength = str(datetime.timedelta(seconds=time.time()-t1))
print('Aggregated', len(allh5), f'files with {num_of_songs} songs', ' in:',stimelength)