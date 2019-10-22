import fs from 'fs';

export default class Sha1Cache {
    constructor(storageDir) {
        this._storageDir = storageDir;
        this._uploadJobs = {};
        this._currentUploadId = 0;
    }
    async findFileForSha1(sha1, opts) {
        opts = opts || {};
        let path = await this._getStoragePathForSha1(sha1);
        try {
            let size = await getFileSize(path);
            let ret = {
                found: true,
                size: size
            };
            if (opts.returnRelPath) {
                ret.relPath = await this._getRelStoragePathForSha1(sha1);
            }
            return ret;
        }
        catch (err) {
            return { found: false };
        }
    }
    directory() {
        return this._storageDir;
    }
    async makeTemporaryUploadFile(sha1) {
        let path = await this._getStoragePathForSha1(sha1, {createDirsIfNeeded: true});
        return path + `.${makeRandomId(6)}.uploading`;
    }
    async moveTemporaryFileIntoPlace(sha1, tmpFileName) {
        let path = await this._getStoragePathForSha1(sha1);
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
    async _getStoragePathForSha1(sha1, opts) {
        opts = opts || {};
        return this._storageDir + '/' + (await this._getRelStoragePathForSha1(sha1, opts));
    }
    async _getRelStoragePathForSha1(sha1, opts) {
        opts = opts || {};
        if (opts.createDirsIfNeeded) {
            await mkdirSafe(this._storageDir + '/' + `${sha1[0]}${sha1[1]}`);
            await mkdirSafe(this._storageDir + '/' + `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}`);
            await mkdirSafe(this._storageDir + '/' + `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}/${sha1[4]}${sha1[5]}`);
        }
        return `${sha1[0]}${sha1[1]}/${sha1[2]}${sha1[3]}/${sha1[4]}${sha1[5]}/${sha1}`;
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