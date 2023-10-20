import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { Readable, Transform } from 'node:stream';
import { WritableStream, TransformStream } from 'node:stream/web';
import csvtojson from 'csvtojson';

const inkCSV = './data/spotify_songs.csv';

createServer(async (request, response) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': '*'
  }

  let chunksReads = 0;
  request.once('close', _ => console.log('connection was closed', chunksReads));
  Readable.toWeb(createReadStream(inkCSV))
    .pipeThrough(Transform.toWeb(csvtojson()))
    .pipeThrough(new TransformStream({
      transform(chunk, controller) {
        const dataChunk = JSON.parse(Buffer.from(chunk));
        const mappedData = {
          Name: dataChunk.track_name,
          Artist: dataChunk.track_artist,
          Popularity: dataChunk.track_popularity,
        };
        controller.enqueue(JSON.stringify(mappedData).concat('\n'));
      }
    }))
    .pipeTo(new WritableStream({
      write(chunk) {
        chunksReads++;
        response.write(chunk);
      },
      close() {
        response.end();
      }
    }));

  response.writeHead(200, headers);
})
  .listen(3000)
  .on('listening', _ => console.log('server is running at', 3000));