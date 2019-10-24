import crypto from 'crypto';
import fs from 'fs';


export default class UploadHandler {
    constructor(hashCache) {
        this._hashCache = hashCache;
        this._uploadJob = null;
        this._writeStream = null;
        this._computedHash = null;
    }
    async handleUpload(hash, fileSize, req, res) {
        this._hash = hash;
        this._fileSize = fileSize;
        this._req = req;
        this._res = res;
        this._tmpFileName = await this._hashCache.makeTemporaryUploadFile(hash);
        this._writeStream = fs.createWriteStream(this._tmpFileName);
        this._computedHash = crypto.createHash(this._hashCache.algorithm());
        await this._handleUploadToTmpFile();
        await this._hashCache.moveTemporaryFileIntoPlace(hash, this._tmpFileName);
    }
    _handleUploadToTmpFile() {
        return new Promise((resolve, reject) => {
            let done = false;
            let numBytesProcessed = 0;
            const doReject = (err) => {
                if (done) return;
                done = true;
                try {
                    this._writeStream.close();
                    if (fs.existsSync(this._tmpFileName)) {
                        fs.unlinkSync(this._tmpFileName);
                    }
                    this._req.connection.destroy();
                }
                catch(err2) {
                    console.warn('Problem cleaning up after upload error.', err2.message);
                }
                reject(err);
            }
            const doResolve = (ret) => {
                if (done) return;
                if (numBytesProcessed != this._fileSize) {
                    doReject(new Error(`Incorrect num bytes processed: ${numBytesProcessed} <> ${this._fileSize}`));
                    return;
                }
                let computedHash = this._computedHash.digest('hex');
                if (computedHash != this._hash) {
                    doReject(new Error(`Computed hash does not match expected: ${computedHash} <> ${this._hash}`));
                    return;
                }
                done = true;
                this._writeStream.close();
                resolve(ret);
            }
            const processChunk = (chunk) => {
                if (done) return;
                numBytesProcessed += chunk.byteLength;
                if (numBytesProcessed > this._fileSize) {
                    doReject(new Error(`Too many bytes processed: ${numBytesProcessed} > ${this._fileSize}. Aborting.`));
                    return;
                }
                this._computedHash.update(chunk);
            }
            this._req.on('data', (chunk) => {
                // hmmm, will all chunks get processed before
                // the write stream emits the 'close' event?
                // not sure, but i hope so, and I think so.
                processChunk(chunk);
            });
            //res.on('end', function() {
            //  // not sure why this is not firing.
            //});
            this._req.on('close', (err) => {
                doReject(new Error('closed'));
            });
            this._req.on('error', (e) => {
                doReject(e);
            });
            this._writeStream.on('error', (err) => {
                doReject(new Error(`Error writing file... ${err.message}`));
            });
            this._writeStream.on('finished', () => {
                // do we resolve here? i don't think so
                doResolve();
            });
            this._writeStream.on('close', () => {
                doResolve();
            });
            this._req.pipe(this._writeStream);
        });
    }
}

