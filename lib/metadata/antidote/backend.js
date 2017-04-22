import { errors } from 'arsenal';

import { markerFilter, prefixFilter } from '../in_memory/bucket_utilities';
import { ListBucketResult } from './ListBucketResult';
import getMultipartUploadListing from '../in_memory/getMultipartUploadListing';
import config from '../../Config';
import async from 'async';
//import antidoteClient from 'antidote_ts_client';
var antidoteCli = require("./antidote_transaction.js") //changer le path


const defaultMaxKeys = 1000;

var CRC32 = require('crc-32');
var HashRing = require('hashring');

var replacer = function(key, value) {
    if (value === undefined){
        return null;
    }
    return value;
};

var reviver = function(key, value) {
    if (value === null){
        return undefined;
    }
    return value;
};


class AntidoteInterface {


/* OLD

    constructor() {
        this.antidote = antidoteClient.connect(config.antidote.port, config.antidote.host);
    }

OLD */


    constructor() {
        this.antidoteCourant={};
        this.antidote=new Array();
        this.tailleTab=config.antidote.length;
        this.hashring = new HashRing();
       //process.stdout.write("\n tailletab="+tailleTab+" \n");
        //process.stdout.write("\n port0="+config.antidote[0].port+" \n");

        for(var i=0; i<this.tailleTab;i++){
          this.hashring.add(config.antidote[i].host+":"+ config.antidote[i].port);
          this.antidote[config.antidote[i].host+":"+ config.antidote[i].port] = antidoteCli.connect(config.antidote[i].port, config.antidote[i].host);


//test
        //  process.stdout.write("\n\n TOUR BOUCLE NUMERO"+i+"\n\n");
          //process.stdout.write("  "+this.antidote[i].port);
        //  process.stdout.write(" \n \n "+config.antidote[i].host+"\n \n");



        }

        //process.stdout.write("\n\n "+this.hashring.get('sazdasdasdasdadalut')+"\n\n ");


        //var crc32 = Math.abs(CRC32.str("sazdasdasdasdadalut"));
        //process.stdout.write(" CRC32 \n \n "+crc32+"\n \n");



        //process.stdout.write("\n\n "+this.hashring.has(config.antidote[0].host+":"+config.antidote[0].port)+"\n\n");
    }


/*
crc32=Math.abs(CRC32.str(bucketName,crc32));
resultatModulo=crc32%tailleTab;
this.antidoteCourant=this.antidote[resultatModulo];
////////// A UTILISER DANS CHAQUE FONCTION CI DESSOUS////////
*/

chooseAntitode(bucketName)
{
        //var crc32 = Math.abs(CRC32.str(bucketName));
        //return (crc32 % tailleTab);
        var serveur=this.hashring.get(bucketName);
        return serveur;
}

/* OLD
    createBucket(bucketName, bucketMD, log, cb) {


        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            // TODO Check whether user already owns the bucket,
            // if so return "BucketAlreadyOwnedByYou"
            // If not owned by user, return "BucketAlreadyExists"
            if (bucket) {
                return cb(errors.BucketAlreadyExists);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_MD = this.antidote.map(`${bucketName}/md`)
            const bucketMap = []
            Object.keys(bucketMD).forEach(key => {
                bucketMap.push(bucket_MD.register(key).set(bucketMD[key]));
            });
            this.antidote.update(bucketMap).then( (resp) => {
                return cb();
            });
        });
    }
*/



    createBucket(bucketName, bucketMD, log, cb) {


      antidoteCli.startTx(antidotedb, (tx) => {



            this.getBucketAttributes(bucketName, log, tx, (tx,err, bucket) => {
                // TODO Check whether user already owns the bucket,
                // if so return "BucketAlreadyOwnedByYou"
                // If not owned by user, return "BucketAlreadyExists"
                if (bucket) {
                    return cb(errors.BucketAlreadyExists);
                }
                var whichAntidote=this.chooseAntitode(bucketName);
                this.antidoteCourant=this.antidote[whichAntidote];

                process.stdout.write("\n \n function createBucket - whichAntidote: " + whichAntidote+" \n \n");

                this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
                let bucket_MD = this.antidoteCourant.map(`${bucketName}/md`)
                const bucketMap = []
                Object.keys(bucketMD).forEach(key => {
                    antidoteCli.updateMapRegister(tx,key,bucketMD[key],(tx) => {
                            antidoteCli.updateSet(tx,key,bucketMD[key],(tx) => {
                    return cb();
                });
            });
            });
            antidoteCli.commitTx(tx, () => {});
        })
        })
    }



/* old

    putBucketAttributes(bucketName, bucketMD, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_MD = this.antidote.map(`${bucketName}/md`)
            const bucketMap = []
            Object.keys(bucketMD).forEach(key => {
                bucketMap.push(bucket_MD.register(key).set(bucketMD[key]))
            });
            this.antidote.update(
                bucketMap
            ).then( (resp) => {
                return cb();
            });
        });
    }

*/

        putBucketAttributes(bucketName, bucketMD, log, cb) {
            this.getBucketAttributes(bucketName, log, err => {
                if (err) {
                    return cb(err);
                }
                            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
                //process.stdout.write("\n \n function putBucketAttributes - antidote_index: " + antidote_index+" \n \n");


                this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
                let bucket_MD = this.antidoteCourant.map(`${bucketName}/md`)
                const bucketMap = []
                Object.keys(bucketMD).forEach(key => {
                    bucketMap.push(bucket_MD.register(key).set(bucketMD[key]))
                });
                this.antidoteCourant.update(
                    bucketMap
                ).then( (resp) => {
                    return cb();
                });
            });
        }








/*old


    getBucketAttributes(bucketName, log, cb) {
        this.antidote.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidote.map(`${bucketName}/md`)
        bucket_MD.read().then(bucketMD => {
            bucketMD = bucketMD.toJsObject();
            if (Object.keys(bucketMD).length === 0) {
                return cb(errors.NoSuchBucket);
            }
            return cb(null, bucketMD);
        });
    }

*/


    getBucketAttributes(bucketName, log, cb,tx) {
            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
        //process.stdout.write("\n \n function getBucketAttributes - antidote_index: " + antidote_index+" \n \n");

        this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidoteCourant.map(`${bucketName}/md`)
        antidoteCli.readSet(tx, bucketName, (tx, result) => {
                bucketMD = result.toJsObject();
                if (Object.keys(bucketMD).length === 0) {
                    return cb(errors.NoSuchBucket);
                }
                console.log(result);
                return cb(null, bucketMD);
            });
    }





/*old


    deleteBucket(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket)  => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            bucket_Objs.read().then(objects => {
                if (bucket && objects.length > 0) {
                    return cb(errors.BucketNotEmpty);
                }
                let bucket_MD = this.antidote.map(`${bucketName}/md`)
                bucket_MD.read().then(bucketMD => {
                    bucketMD = bucketMD.toJsObject();
                    const bucketMap = []
                    Object.keys(bucketMD).forEach(key => {
                        bucket_MD.remove(bucket_MD.register(key))
                    });
                    this.antidote.update(bucketMap).then( (resp) => {
                        return cb(null);
                    });
                });
            });
        });
    }
*/


    deleteBucket(bucketName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket)  => {
            if (err) {
                return cb(err);
            }

            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
            process.stdout.write("\n \n function deleteBucket - whichAntidote: " + whichAntidote+" \n \n");


            this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidoteCourant.set(`${bucketName}/objs`);
            bucket_Objs.read().then(objects => {
                if (bucket && objects.length > 0) {
                    return cb(errors.BucketNotEmpty);
                }
                let bucket_MD = this.antidoteCourant.map(`${bucketName}/md`)
                bucket_MD.read().then(bucketMD => {
                    bucketMD = bucketMD.toJsObject();
                    const bucketMap = []
                    Object.keys(bucketMD).forEach(key => {
                        bucket_MD.remove(bucket_MD.register(key))
                    });
                    this.antidoteCourant.update(bucketMap).then( (resp) => {
                        return cb(null);
                    });
                });
            });
        });
    }










/*old

    putObject(bucketName, objName, objVal, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }
                this.antidote.defaultBucket = `storage/${bucketName}`;
                let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
                let object_MD = this.antidote.map(`${objName}`);
                const objMap = []
                Object.keys(objVal).forEach(key => {
                    objMap.push(object_MD.register(key).set(objVal[key]))
                });
                objMap.push(bucket_Objs.add(objName))
                this.antidote.update(objMap).then( (resp) => {
                    return cb();
                });
            });
    }
*/


    putObject(bucketName, objName, objVal, log, cb) {
        this.getBucketAttributes(bucketName, log, err => {
            if (err) {
                return cb(err);
            }

            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
            //process.stdout.write("\n \n function putObject - antidote_index: " + antidote_index+" \n \n");


                this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
                let bucket_Objs = this.antidoteCourant.set(`${bucketName}/objs`);
                let object_MD = this.antidoteCourant.map(`${objName}`);
                const objMap = []
                Object.keys(objVal).forEach(key => {
                    objMap.push(object_MD.register(key).set(objVal[key]))
                });
                objMap.push(bucket_Objs.add(objName))
                this.antidoteCourant.update(objMap).then( (resp) => {
                    return cb();
                });
            });
    }




/*old
    getBucketAndObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err, { bucket });
            }
            const bucket_MD = {}
            Object.keys(bucket).map(function(key) {
                bucket_MD[key.substr(1)] = bucket[key]
            });
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();

                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(null, { bucket: JSON.stringify(bucket_MD) });
                }
                return cb(null, {
                    bucket: JSON.stringify(bucket_MD),
                    obj: JSON.stringify(objectMD),
                });
            });
        });
    }
*/



    getBucketAndObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err, { bucket });
            }

            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
            //process.stdout.write("\n \n function getBucketAndObject - antidote_index: " + antidote_index+" \n \n");


            const bucket_MD = {}
            Object.keys(bucket).map(function(key) {
                bucket_MD[key.substr(1)] = bucket[key]
            });
            this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidoteCourant.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();

                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(null, { bucket: JSON.stringify(bucket_MD) });
                }
                return cb(null, {
                    bucket: JSON.stringify(bucket_MD),
                    obj: JSON.stringify(objectMD),
                });
            });
        });
    }






/* old

    getObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                return cb(null, objectMD);
            });
        });
    }
*/





    getObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }

            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
            //process.stdout.write("\n \n function getObject - antidote_index: " + antidote_index+" \n \n");


            this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidoteCourant.map(`${objName}`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                return cb(null, objectMD);
            });
        });
    }








/* old

    deleteObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            this.antidote.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidote.map(`${objName}`);
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                const objMap = []
                Object.keys(objectMD).forEach(key => {
                    objMap.push(object_MD.remove(object_MD.register(key)))
                });
                objMap.push(bucket_Objs.remove(objName));
                this.antidote.update(objMap).then( (resp) => {
                    return cb();
                });
            });
        });
    }
*/


    deleteObject(bucketName, objName, log, cb) {
        this.getBucketAttributes(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }

            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
            //process.stdout.write("\n \n function deleteObject - antidote_index: " + antidote_index+" \n \n");


            this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
            let object_MD = this.antidoteCourant.map(`${objName}`);
            let bucket_Objs = this.antidoteCourant.set(`${bucketName}/objs`);
            object_MD.read().then(objectMD => {
                objectMD = objectMD.toJsObject();
                if (!bucket || Object.keys(objectMD).length === 0) {
                    return cb(errors.NoSuchKey);
                }
                const objMap = []
                Object.keys(objectMD).forEach(key => {
                    objMap.push(object_MD.remove(object_MD.register(key)))
                });
                objMap.push(bucket_Objs.remove(objName));
                this.antidoteCourant.update(objMap).then( (resp) => {
                    return cb();
                });
            });
        });
    }












    getObjectMD(antidote, bucketName, key, callback) {
        antidote.defaultBucket = `storage/${bucketName}`;
        let object_MD = antidote.map(`${key}`);
        object_MD.read().then(objectMD => {
            objectMD = objectMD.toJsObject();
            if (Object.keys(objectMD).length === 0) {
                return callback(error.NoSuchKey, null);
            }
            return callback(null, objectMD);
        });
    }


/* old
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

        this.antidote.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidote.map(`${bucketName}/md`)
        bucket_MD.read().then(bucketMD => {
            bucketMD = bucketMD.toJsObject();
            if (Object.keys(bucketMD).length === 0) {
                return cb(errors.NoSuchBucket);
            }
            const response = new ListBucketResult();

            this.antidote.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidote.set(`${bucketName}/objs`);
            bucket_Objs.read().then(keys => {

                async.map(keys, this.getObjectMD.bind(null, this.antidote, bucketName), function(err, objectMeta) {

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
    }
*/



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

        //process.stdout.write(" \n \n \n "+tailleTab+"\n \n");
        //process.stdout.write(" \n \n \n "+this.tailleTab+"\n \n");
            var whichAntidote=this.chooseAntitode(bucketName);
            this.antidoteCourant=this.antidote[whichAntidote];
       // process.stdout.write("\n \n function listObject - antidote_index: " + antidote_index+" \n \n");




        this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
        let bucket_MD = this.antidoteCourant.map(`${bucketName}/md`)
        bucket_MD.read().then(bucketMD => {
            bucketMD = bucketMD.toJsObject();
            if (Object.keys(bucketMD).length === 0) {
                return cb(errors.NoSuchBucket);
            }
            const response = new ListBucketResult();

            this.antidoteCourant.defaultBucket = `storage/${bucketName}`;
            let bucket_Objs = this.antidoteCourant.set(`${bucketName}/objs`);
            bucket_Objs.read().then(keys => {

                async.map(keys, this.getObjectMD.bind(null, this.antidoteCourant, bucketName), function(err, objectMeta) {

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
    }








    listMultipartUploads(bucketName, listingParams, log, cb) {
        process.nextTick(() => {
            this.getBucketAttributes(bucketName, log, (err, bucket) => {
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
