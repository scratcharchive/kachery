import fs from 'fs';

export default class HashCache {
    constructor(cacheDir, algorithm, indexer) {
        this._cacheDir = cacheDir;
        this._algorithm = algorithm;
        this._indexer = indexer;
        this._uploadJobs = {};
        this._currentUploadId = 0;
    }
    algorithm() {
        return this._algorithm;
    }
    async findFileForHash(hash, opts) {
        opts = opts || {};
        let path = await this._getStoragePathForHash(hash);
        try {
            let size = await getFileSize(path);
            let ret = {
                found: true,
                size: size
            };
            if (opts.returnPath) {
                ret.relPath = await this._getRelStoragePathForHash(hash);
                ret.basePath = this._cacheDir;
            }
            return ret;
        }
        catch (err) {
            if ((this._algorithm == 'sha1') && (this._indexer)) {
                let result = await this._indexer.findFileForSha1(hash);
                if (result.found) {
                    let ret = {
                        found: true,
                        size: result.size,
                    };
                    if (opts.returnPath) {
                        ret.relPath = result.relPath;
                        ret.basePath = this._indexer.baseDirectory();
                    }
                    return ret;
                }
                else {
                    return {found: false};
                }
            }
            else {
                return { found: false };
            }
        }
    }
    directory() {
        return this._cacheDir;
    }
    async makeTemporaryUploadFile(hash) {
        let path = await this._getStoragePathForHash(hash, {createDirsIfNeeded: true});
        return path + `.${makeRandomId(6)}.uploading`;
    }
    async moveTemporaryFileIntoPlace(hash, tmpFileName) {
        let path = await this._getStoragePathForHash(hash);
        if (fs.existsSync(path)) {
            fs.unlinkSync(tmpFileName);
            return; // it is already there
        }
        try {
            await fs.promises.rename(tmpFileName, path);
        }
        catch(err) {
            if (fs.existsSync(path)) {
                fs.unlinkSync(tmpFileName);
                return; // it is already there
            }
            throw new Error(`Problem renaming file ${tmpFileName} -> ${path}`);
        }
    }
    async _getStoragePathForHash(hash, opts) {
        opts = opts || {};
        return this._cacheDir + '/' + (await this._getRelStoragePathForHash(hash, opts));
    }
    async _getRelStoragePathForHash(hash, opts) {
        opts = opts || {};
        if (opts.createDirsIfNeeded) {
            await mkdirSafe(this._cacheDir + '/' + `${hash[0]}${hash[1]}`);
            await mkdirSafe(this._cacheDir + '/' + `${hash[0]}${hash[1]}/${hash[2]}${hash[3]}`);
            await mkdirSafe(this._cacheDir + '/' + `${hash[0]}${hash[1]}/${hash[2]}${hash[3]}/${hash[4]}${hash[5]}`);
        }
        return `${hash[0]}${hash[1]}/${hash[2]}${hash[3]}/${hash[4]}${hash[5]}/${hash}`;
    }
}

async function mkdirSafe(path) {
    try {
        await fs.promises.mkdir(path);
    }
    catch(err) {
        //
    }
}

async function getFileSize(path) {
    let stat = await fs.promises.stat(path);
    return stat.size;
}

function makeRandomId(num_chars) {
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for (var i = 0; i < num_chars; i++)
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    return text;
  }