export default class DownloadHandler {
    constructor(hashCache) {
        this._hashCache = hashCache;
    }
    async handleDownload(hash, req, res) {
        let result = await this._hashCache.findFileForHash(hash, {returnPath: true});
        if (!result.found) {
            throw new Error('File not found.');
        }

        await sendFile(res, result.relPath, this._hashCache.directory());
    }
}

function sendFile(res, path, root) {
    return new Promise((resolve, reject) => {
        res.sendFile(path, {
            root: root
        }, function(err) {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        })
    });
}
