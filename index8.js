/* eslint-disable no-console */

// Dependencies
const fs = require('fs');
const pull = require('pull-stream');
const { read } = require('pull-files');
const Sort = require('pull-sort');
const Group = require('pull-group');
const Tee = require('pull-tee');
const Write = require('pull-write-file');

// Constants
const inputFilesGlob = 'input_files/**/*.dat';
const eightStreamFiles = [
  'stream1.dat',
  'stream2.dat',
  'stream3.dat',
  'stream4.dat',
  'stream5.dat',
  'stream6.dat',
  'stream7.dat',
  'stream8.dat',
];

const compareDataFiles = (df1, df2) => {
  if (df1.fileSize > df2.fileSize) {
    return -1;
  }
  if (df1.fileSize < df2.fileSize) {
    return 1;
  }
  return 0;
};

let counter = 0;

pull(
  read(inputFilesGlob, { cwd: __dirname }),
  pull.asyncMap((file, cb) => {
    fs.stat(`${file.base}/${file.path}`, (err, stats) => {
      const dataFile = {
        fileName: file.path,
        fileSize: stats.size,
        fileData: file.data,
      };
      cb(null, dataFile);
    });
  }),
  Sort(compareDataFiles),
  Group(8),
  pull.collect((err, groups) => {
    console.log('groups', groups);
    pull(
      pull.values(groups),
      Tee(
        pull.collect((err1, groups1) => {
          if (err1) {
            console.log('err1', err1);
          }
          // console.log(group1);
          pull(
            pull.values(groups1),
            pull.map(data => data[0].fileData),
            Write(`${eightStreamFiles[0]}`, {}, () => {
              console.log('AAA');
            })
          );
        })
      ),
      Tee(
        pull.collect((err2, groups2) => {
          if (err2) {
            console.log('err2', err2);
          }
          pull(
            pull.values(groups2),
            pull.map(data => data[1].fileData),
            Write(`${eightStreamFiles[1]}`, {}, () => {
              console.log('BBB');
            })
          );
        })
      ),
      Tee(
        pull.collect((err3, groups3) => {
          if (err3) {
            console.log('err3', err3);
          }
          pull(
            pull.values(groups3),
            pull.map(data => data[2].fileData),
            Write(`${eightStreamFiles[2]}`, {}, () => {
              console.log('CCC');
            })
          );
        })
      ),
      Tee(
        pull.collect((err4, groups4) => {
          if (err4) {
            console.log('err1', err4);
          }
          pull(
            pull.values(groups4),
            pull.map(data => data[3].fileData),
            Write(`${eightStreamFiles[3]}`, {}, () => {
              console.log('DDD');
            })
          );
        })
      ),
      Tee(
        pull.collect((err5, groups5) => {
          if (err5) {
            console.log('err1', err5);
          }
          // console.log(group1);
          pull(
            pull.values(groups5),
            pull.map(data => data[4].fileData),
            Write(`${eightStreamFiles[4]}`, {}, () => {
              console.log('EEE');
            })
          );
        })
      ),
      Tee(
        pull.collect((err6, groups6) => {
          if (err6) {
            console.log('err2', err6);
          }
          pull(
            pull.values(groups6),
            pull.map(data => data[5].fileData),
            Write(`${eightStreamFiles[5]}`, {}, () => {
              console.log('FFF');
            })
          );
        })
      ),
      Tee(
        pull.collect((err7, groups7) => {
          if (err7) {
            console.log('err3', err7);
          }
          pull(
            pull.values(groups7),
            pull.map(data => data[6].fileData),
            Write(`${eightStreamFiles[6]}`, {}, () => {
              console.log('GGG');
            })
          );
        })
      ),
      Tee(
        pull.collect((err8, groups8) => {
          if (err8) {
            console.log('err1', err8);
          }
          pull(
            pull.values(groups8),
            pull.map(data => data[7].fileData),
            Write(`${eightStreamFiles[7]}`, {}, () => {
              console.log('HHH');
            })
          );
        })
      ),
      pull.drain(
        () => {
          counter += 1;
          console.log('Group #', counter);
        },
        () => {
          console.log('Done');
        }
      )
    );
  })
);
