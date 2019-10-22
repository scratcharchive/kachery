export default class DownloadHandler {
    constructor(sha1Cache) {
        this._sha1Cache = sha1Cache;
    }
    async handleDownload(sha1, req, res) {
        let result = await this._sha1Cache.findFileForSha1(sha1, {returnRelPath: true});
        if (!result.found) {
            throw new Error('File not found.');
        }

        await sendFile(res, result.relPath, this._sha1Cache.directory());
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
