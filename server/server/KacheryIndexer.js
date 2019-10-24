import fs from 'fs';
import crypto from 'crypto';

export default class KacheryIndexer {
    constructor(storageDir) {
        this._storageDir = storageDir;
        mkdirIfNeeded(this._indexDir());
        mkdirIfNeeded(this._storageDir + '/data');
    }
    startIndexing() {
        this._nextIndexIteration();
    }
    baseDirectory() {
        return this._storageDir;
    }
    async findFileForSha1(sha1) {
        let path0 = await this._getRelJsonPathForSha1(sha1, { createDirsIfNeeded: false });
        let obj = await readJsonSafe(this._indexDir() + '/' + path0);
        if (!obj) {
            return { found: false };
        }
        let fname = this._storageDir + '/' + obj.relPath;
        let fileStats1 = obj.fileStats;
        let fileStats2 = await getFileStatsSafe(fname);
        if ((fileStats2) && (deepEqual(fileStats1, fileStats2))) {
            return {
                found: true,
                size: fileStats2.size,
                relPath: obj.relPath
            };
        }
        else {
            return { found: false };
        }
    }
    async _getRelJsonPathForSha1(sha1, opts) {
        opts = opts || {};
        if (opts.createDirsIfNeeded) {
            await mkdirSafe(this._indexDir() + '/' + `${sha1[0]}${sha1[1]}`);
            await mkdirSafe(this._indexDir() + '/' + `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}`);
            await mkdirSafe(this._indexDir() + '/' + `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}/${sha1[4]}${sha1[5]}`);
        }
        return `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}/${sha1[4]}${sha1[5]}/${sha1}.json`;
    }
    async _nextIndexIteration() {
        await this._doIndexIteration();
        setTimeout(async () => {
            await this._nextIndexIteration();
        }, 3000);
    }
    async _doIndexIteration() {
        try {
            await this._doIndexDirectory('data');
        }
        catch (err) {
            console.error(err);
            console.warn('Indexing error: ', err.message);
        }
    }
    async _doIndexDirectory(relPath) {
        let absPath = this._storageDir + '/' + relPath;
        let entries = await fs.promises.readdir(absPath);
        for (let entry of entries) {
            let absPath2 = absPath + '/' + entry;
            let relPath2 = relPath ? relPath + '/' + entry : entry;
            let stats = await fs.promises.lstat(absPath2);
            if (stats.isFile()) {
                if (this._okayToIndexFile(entry)) {
                    try {
                        await this._doIndexFile(relPath2);
                    }
                    catch (err) {
                        console.error(err);
                        console.warn(`Problem indexing file ${relPath2}: ${err.message}`);
                    }
                }
            }
            else if (stats.isDirectory()) {
                if (this._okayToIndexDirectory(entry)) {
                    try {
                        await this._doIndexDirectory(relPath2)
                    }
                    catch (err) {
                        console.error(err);
                        console.warn(`Problem indexing directory ${relPath2}: ${err.message}`);
                    }
                }
            }
        }
    }
    _okayToIndexDirectory(name) {
        let exclude = ['.git'];
        exclude.push('sha1-cache');
        exclude.push('md5-cache');
        exclude.push('.kachery');
        if (exclude.indexOf(name) >= 0)
            return false;
        return true;
    }
    _okayToIndexFile(name) {
        return true;
    }
    async _doIndexFile(relPath) {
        let absPath = this._storageDir + '/' + relPath;
        let hash = sha1OfString(relPath);
        let path0 = await this._getRelJsonPathForSha1(hash);
        let obj = await readJsonSafe(this._indexDir() + '/' + path0);
        if (obj) {
            let fileStats = await getFileStatsSafe(absPath);
            if ((fileStats) && (deepEqual(obj.fileStats, fileStats))) {
                // already indexed
                let sha1 = obj.sha1;
                let path1 = await this._getRelJsonPathForSha1(sha1);
                let obj1 = await readJsonSafe(this._indexDir() + '/' + path1);
                if ((deepEqual(obj1.fileStats, fileStats)) && (obj1.relPath == relPath)) {
                    // yes everything is already done, no need to update
                    return;
                }
            }
        }
        // i guess we need to actually do the indexing for this file.
        let fileStatsBefore = await getFileStatsSafe(absPath);
        console.info(`COMPUTING SHA-1: ${absPath}`);
        let fileSha1 = await computeFileSha1(absPath);
        console.info(`SHA-1 of ${absPath}: ${fileSha1}`);
        let fileStatsAfter = await getFileStatsSafe(absPath);
        if (!deepEqual(fileStatsBefore, fileStatsAfter)) {
            // Unfortunately the file has changed while we we were reading it.
            return;
        }
        let newObj = {
            relPath: relPath,
            fileStats: fileStatsAfter,
            sha1: fileSha1
        };
        let aa = await this._getRelJsonPathForSha1(hash, { createDirsIfNeeded: true });
        let bb = await this._getRelJsonPathForSha1(fileSha1, { createDirsIfNeeded: true });
        await writeJsonSafe(this._indexDir() + '/' + aa, newObj);
        await writeJsonSafe(this._indexDir() + '/' + bb, newObj);
    }

    _indexDir() {
        return this._storageDir + '/.kachery';
    }
}

function sha1OfString(txt) {
    let shasum = crypto.createHash('sha1');
    shasum.update(txt);
    return shasum.digest('hex');
}

function computeFileSha1(filename, algorithm = 'sha1') {
    return new Promise((resolve, reject) => {
        // Algorithm depends on availability of OpenSSL on platform
        // Another algorithms: 'sha1', 'md5', 'sha256', 'sha512' ...
        let shasum = crypto.createHash(algorithm);
        try {
            let s = fs.ReadStream(filename)
            s.on('data', function (data) {
                shasum.update(data)
            })
            // making digest
            s.on('end', function () {
                const hash = shasum.digest('hex')
                return resolve(hash);
            })
        } catch (error) {
            return reject('Failure in SHA-1 hash calculation');
        }
    });
}

async function mkdirSafe(path) {
    try {
        await fs.promises.mkdir(path);
    }
    catch (err) {
        //
    }
}

function deepEqual(x, y) {
    const ok = Object.keys, tx = typeof x, ty = typeof y;
    return x && y && tx === 'object' && tx === ty ? (
        ok(x).length === ok(y).length &&
        ok(x).every(key => deepEqual(x[key], y[key]))
    ) : (x === y);
}

async function readJsonSafe(filePath) {
    try {
        const txt = await fs.promises.readFile(filePath);
        return JSON.parse(txt);
    }
    catch (err) {
        return null;
    }
}

async function writeJsonSafe(path, obj) {
    try {
        await fs.promises.writeFile(path, JSON.stringify(obj));
    }
    catch (err) {
        console.warn(`Problem writing json file: ${path}`);
    }
}

async function getFileStatsSafe(path) {
    try {
        let stats = await fs.promises.lstat(path);
        return {
            size: stats.size,
            ino: stats.ino,
            mtime: stats.mtime + ''
        };
    }
    catch (err) {
        return null;
    }

}


function mkdirIfNeeded(path) {
    if (!fs.existsSync(path)) {
        fs.mkdirSync(path);
    }
}