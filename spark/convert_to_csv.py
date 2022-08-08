import time
from tqdm import tqdm

import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader


def convert_to_csv(path, schemaFile, output):
    schema = avro.schema.parse(open(schemaFile, "rb").read())
    reader = DataFileReader(open(path, "rb"), DatumReader(readers_schema=schema))

    t1 = time.time()

    with open(output, 'w+') as out:
        for song in tqdm(reader):
            artist = song['artist_id'].decode()
            song_id = song['song_id'].decode()
            
            similar_artists = list(set(song['similar_artists'].decode().split(',')))
            similar_all = ",".join(similar_artists)

            print(song_id, artist, similar_all, sep=',', file=out)
    
    print(f'{time.time() - t1:.3f} seconds passes')

convert_to_csv('song.avro', 'song.avsc', 'res.csv')
