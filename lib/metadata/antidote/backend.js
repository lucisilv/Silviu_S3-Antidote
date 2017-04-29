import { errors } from 'arsenal';

import { markerFilter, prefixFilter } from '../in_memory/bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import getMultipartUploadListing from '../in_memory/getMultipartUploadListing';
import config from '../../Config';
import async from 'async';
//import antidoteClient from 'antidote_ts_client';
import antidoteCli from './antidote_transaction';

const defaultMaxKeys = 1000;
var CRC32 = require('crc-32');
var HashRing = require('hashring');
var locks = require('locks');
var rwlock = locks.createReadWriteLock();

class AntidoteInterface {
    constructor() {
        //this.antidotedb = antidoteCli.connect(config.antidote.port, config.antidote.host);
        this.antidoteCourant={};
        this.antidotedb=new Array();
        this.tailleTab=config.antidote.length;
        this.hashring = new HashRing();
       //process.stdout.write("\n tailletab="+tailleTab+" \n");
        //process.stdout.write("\n port0="+config.antidote[0].port+" \n");

        for(var i=0; i<this.tailleTab;i++){
          this.hashring.add(config.antidote[i].host+":"+ config.antidote[i].port);
          this.antidotedb[config.antidote[i].host+":"+ config.antidote[i].port] = antidoteCli.connect(config.antidote[i].port, config.antidote[i].host);



        //  process.stdout.write("\n\n TOUR BOUCLE NUMERO"+i+"\n\n");
          //process.stdout.write("  "+this.antidote[i].port);
        //  process.stdout.write(" \n \n "+config.antidote[i].host+"\n \n");



        }
    }

    chooseAntitode(bucketName)
    {
            //var crc32 = Math.abs(CRC32.str(bucketName));
            //return (crc32 % tailleTab);
            var serveur=this.hashring.get(bucketName);
            return serveur;
    }

    createBucket(bucketName, bucketMD, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, (err, bucket) => {
                // TODO Check whether user already owns the bucket,
                // if so return "BucketAlreadyOwnedByYou"
                // If not owned by user, return "BucketAlreadyExists"
                if (bucket) {
                    return cb(errors.BucketAlreadyExists);
                }
                const mapKeys = []
                const mapValues = []
                Object.keys(bucketMD).forEach(key => {
                    mapKeys.push(key)
                    mapValues.push(bucketMD[key])
                });
//write clock
//                rwlock.writeLock(function cb() {
//            	console.log('WriteLock!');
            	// do stuff
                antidoteCli.updateMapRegister(tx, `${bucketName}/md`, mapKeys, mapValues, (tx) => {
                    antidoteCli.commitTx(tx, () => {
                        return cb();
                    });
                });
//              rwlock.unlock();
//              });
            });
        });
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                const mapKeys = []
                const mapValues = []
                Object.keys(bucketMD).forEach(key => {
                    mapKeys.push(key)
                    mapValues.push(bucketMD[key])
                });
                antidoteCli.updateMapRegister(tx, `${bucketName}/md`, mapKeys, mapValues, (tx) => {
                    antidoteCli.commitTx(tx, () => {
                        return cb();
                    });
                });
            });
        });
    }

    getBucketAttributes(tx, bucketName, log, cb) {
        if (tx === null) {
            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidotedb[whichAntidote];
            antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
//read clock
//            rwlock.readLock(function cb() {
//            console.log('readLock');
            antidoteCli.startTx(this.antidoteCourant, (tx) => {
                antidoteCli.readMap(tx, `${bucketName}/md`, (tx, bucketMD) => {
                    antidoteCli.commitTx(tx, () => {
                        if (Object.keys(bucketMD).length === 0) {
                            return cb(errors.NoSuchBucket);
                        }
                        return cb(null, bucketMD);
                    });
                });
            });
//            rwlock.unlock();
//            });
        }
        else {
//read clock
//			rwlock.readLock(function cb() {
//            console.log('readLock');
            antidoteCli.readMap(tx, `${bucketName}/md`, (tx, bucketMD) => {
                if (Object.keys(bucketMD).length === 0) {
                    return cb(errors.NoSuchBucket);
                }
                return cb(null, bucketMD);
            });
        }
//		 rwlock.unlock();
//            });
    }

    deleteBucket(bucketName, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, (err, bucket)  => {
                if (err) {
                    return cb(err);
                }
                antidoteCli.readSet(tx, `${bucketName}/objs`, (tx, objects) => {
                    if (bucket && objects.length > 0) {
                        antidoteCli.commitTx(tx, () => {
                            return cb(errors.BucketNotEmpty);
                        });
                    }
                    antidoteCli.readMap(tx, `${bucketName}/md`, (tx, bucketMD) => {
                        const mapKeys = []
                        Object.keys(bucketMD).forEach(key => {
                            mapKeys.push(key)
                        });
                        antidoteCli.removeMapRegister(tx, `${bucketName}/md`, mapKeys, (tx) => {
                            antidoteCli.commitTx(tx, () => {
                                return cb(null);
                            });
                        });
                    });
                });
            });
        });
    }

    putObject(bucketName, objName, objVal, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                const mapKeys = []
                const mapValues = []
                Object.keys(objVal).forEach(key => {
                    mapKeys.push(key)
                    mapValues.push(objVal[key])
                });
                antidoteCli.updateSet(tx, `${bucketName}/objs`, [objName], (tx) => {
                    antidoteCli.updateMapRegister(tx, `${objName}`, mapKeys, mapValues, (tx) => {
                        antidoteCli.commitTx(tx, () => {
                            return cb();
                        });
                    });
                });
            });
        });
    }

    getBucketAndObject(bucketName, objName, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, (err, bucket) => {
                if (err) {
                    return cb(err, { bucket });
                }
                const bucket_MD = {}
                Object.keys(bucket).map(function(key) {
                    bucket_MD[key.substr(1)] = bucket[key]
                });
                antidoteCli.readMap(tx, `${objName}`, (tx, objectMD) => {
                    antidoteCli.commitTx(tx, () => {
                        if (!bucket || Object.keys(objectMD).length === 0) {
                            return cb(null, { bucket: JSON.stringify(bucket_MD) });
                        }
                        return cb(null, {
                            bucket: JSON.stringify(bucket_MD),
                            obj: JSON.stringify(objectMD),
                        });
                    });
                });
            });
        });
    }

    getObject(bucketName, objName, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, (err, bucket) => {
                if (err) {
                    return cb(err);
                }
                antidoteCli.readMap(tx, `${objName}`, (tx, objectMD) => {
                    antidoteCli.commitTx(tx, () => {
                        if (!bucket || Object.keys(objectMD).length === 0) {
                            return cb(errors.NoSuchKey);
                        }
                        return cb(null, objectMD);
                    });
                });
            });
        });
    }

    deleteObject(bucketName, objName, log, cb) {
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            this.getBucketAttributes(tx, bucketName, log, (err, bucket) => {
                if (err) {
                    return cb(err);
                }
                antidoteCli.readMap(tx, `${objName}`, (tx, objectMD) => {
                    if (!bucket || Object.keys(objectMD).length === 0) {
                        antidoteCli.commitTx(tx, () => {
                            return cb(errors.NoSuchKey);
                        });
                    }
                    const mapKeys = []
                    Object.keys(objectMD).forEach(key => {
                        mapKeys.push(key)
                    });
                    antidoteCli.removeMapRegister(tx, `${objName}`, mapKeys, (tx) => {
                        antidoteCli.removeSet(tx, `${bucketName}/objs`, [objName], (tx) => {
                            antidoteCli.commitTx(tx, () => {
                                    return cb();
                            });
                        });
                    });
                });
            });
        });
    }

    getObjectMD(tx, bucketName, key, callback) {
        antidoteCli.readMap(tx, `${key}`, (tx, objectMD) => {
            if (Object.keys(objectMD).length === 0) {
                return callback(error.NoSuchKey, null);
            }
            return callback(null, objectMD);
        });
    }

    listObject(bucketName, params, log, cb) {
        const { prefix, marker, delimiter, maxKeys } = params;
        if (prefix && typeof prefix !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (marker && typeof marker !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (delimiter && typeof delimiter !== 'string') {
            return cb(errors.InvalidArgument);
        }

        if (maxKeys && typeof maxKeys !== 'number') {
            return cb(errors.InvalidArgument);
        }

        let numKeys = maxKeys;
        // If paramMaxKeys is undefined, the default parameter will set it.
        // However, if it is null, the default parameter will not set it.
        if (numKeys === null) {
            numKeys = defaultMaxKeys;
        }
        var whichAntidote=this.chooseAntitode(bucketName);
        this.antidoteCourant=this.antidotedb[whichAntidote];
        antidoteCli.setBucket(this.antidoteCourant, `storage/${bucketName}`);
        antidoteCli.startTx(this.antidoteCourant, (tx) => {
            antidoteCli.readMap(tx, `${bucketName}/md`, (tx, bucketMD) => {
                if (Object.keys(bucketMD).length === 0) {
                    return cb(errors.NoSuchBucket);
                }
                const response = new ListBucketResult();
                antidoteCli.readSet(tx, `${bucketName}/objs`, (tx, keys) => {
                    async.map(keys, this.getObjectMD.bind(null, tx, bucketName), function(err, objectMeta) {
                        antidoteCli.commitTx(tx, () => {
                            // If marker specified, edit the keys array so it
                            // only contains keys that occur alphabetically after the marker
                            if (marker) {
                                keys = markerFilter(marker, keys);
                                response.Marker = marker;
                            }
                            // If prefix specified, edit the keys array so it only
                            // contains keys that contain the prefix
                            if (prefix) {
                                keys = prefixFilter(prefix, keys);
                                response.Prefix = prefix;
                            }
                            // Iterate through keys array and filter keys containing
                            // delimiter into response.CommonPrefixes and filter remaining
                            // keys into response.Contents
                            for (let i = 0; i < keys.length; ++i) {
                                const currentKey = keys[i];
                                // Do not list object with delete markers
                                if (response.hasDeleteMarker(currentKey,
                                    objectMeta[i])) {
                                    continue;
                                }
                                // If hit numKeys, stop adding keys to response
                                if (response.MaxKeys >= numKeys) {
                                    response.IsTruncated = true;
                                    response.NextMarker = keys[i - 1];
                                    break;
                                }
                                // If a delimiter is specified, find its index in the
                                // current key AFTER THE OCCURRENCE OF THE PREFIX
                                let delimiterIndexAfterPrefix = -1;
                                let prefixLength = 0;
                                if (prefix) {
                                    prefixLength = prefix.length;
                                }
                                const currentKeyWithoutPrefix = currentKey
                                    .slice(prefixLength);
                                let sliceEnd;
                                if (delimiter) {
                                    delimiterIndexAfterPrefix = currentKeyWithoutPrefix
                                        .indexOf(delimiter);
                                    sliceEnd = delimiterIndexAfterPrefix + prefixLength;
                                    response.Delimiter = delimiter;
                                }
                                // If delimiter occurs in current key, add key to
                                // response.CommonPrefixes.
                                // Otherwise add key to response.Contents
                                if (delimiterIndexAfterPrefix > -1) {
                                    const keySubstring = currentKey.slice(0, sliceEnd + 1);
                                    response.addCommonPrefix(keySubstring);
                                } else {
                                    response.addContentsKey(currentKey,
                                        objectMeta[i]);
                                }
                            }
                            return cb(null, response);
                        });
                    });
                });
            });
        });
    }

    listMultipartUploads(bucketName, listingParams, log, cb) {
        process.nextTick(() => {
            this.getBucketAttributes(null, bucketName, log, (err, bucket) => {
                if (bucket === undefined) {
                    // no on going multipart uploads, return empty listing
                    return cb(null, {
                        IsTruncated: false,
                        NextMarker: undefined,
                        MaxKeys: 0,
                    });
                }
                return getMultipartUploadListing(bucket, listingParams, cb);
            });
        });
    }
};

export default AntidoteInterface;
