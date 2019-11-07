import express from 'express';
import https from 'https';
import http from 'http';
import HashCache from './HashCache.js';
import DownloadHandler from './DownloadHandler.js';
import UploadHandler from './UploadHandler.js';
import KacheryTaskRegulator from './KacheryTaskRegulator.js';
import KacheryIndexer from './KacheryIndexer.js';
import fs from 'fs';

export default class KacheryServer {
    constructor(storageDir) {
        const config = readJsonFile(storageDir + '/kachery.json');
        mkdirIfNeeded(storageDir + '/sha1-cache');
        mkdirIfNeeded(storageDir + '/md5-cache');
        this._storageDir = storageDir;
        this._regulator = new KacheryTaskRegulator(config);;
        this._indexer = new KacheryIndexer(this._storageDir);
        this._caches = {
            sha1: new HashCache(this._storageDir + '/sha1-cache', 'sha1', this._indexer),
            md5: new HashCache(this._storageDir + '/md5-cache', 'md5', this._indexer)
        };

        this._app = express(); // the express app

        this._app.set('json spaces', 4); // when we respond with json, this is how it will be formatted
        // this._app.use(cors()); // in the future, if we want to do this
        this._app.use(express.json());

        this._app.get('/probe', async (req, res) => {
            await waitMsec(1000);
            try {
                await this._apiProbe(req, res) 
            }
            catch(err) {
                await this._errorResponse(req, res, 500, err.message);
            }
        });
        this._app.get('/check/:algorithm/:hash', async (req, res) => {
            let approvalObject = await this._approveTask('check', req.query.channel, req.params.algorithm, req.params.hash, null, req.query.signature, req);
            if (!approvalObject.approve) {
                await this._errorResponse(req, res, 500, approvalObject.reason);
                return;
            }
            try {
                await this._apiCheck(req, res)
            }
            catch(err) {
                await this._errorResponse(req, res, 500, err.message);
            }
            finally {
                this._finalizeTask('check', req.query.channel, null, approvalObject);
            }
        });
        this._app.get('/get/:algorithm/:hash', async (req, res) => {
            ///////////////////////////////////////////////////////////////////////
            // First we need to do a check to determine the file size
            let params = req.params;
            if (!validateHashString(params.algorithm, params.hash)) {
                await this._errorResponse(req, res, 500, `Invalid ${params.algorithm} string`);
                return;
            }
            let result = await this._caches[params.algorithm].findFileForHash(params.hash);
            if (!result.found) {
                await this._errorResponse(req, res, 500, 'Not found.');
                return;
            }
            let numBytes = result.size;
            ///////////////////////////////////////////////////////////////////////

            let approvalObject = await this._approveTask('download', req.query.channel, req.params.algorithm, req.params.hash, numBytes, req.query.signature, req);
            if (!approvalObject.approve) {
                await this._errorResponse(req, res, 500, approvalObject.reason);
                return;
            }
            try {
                await this._apiGet(req, res);
                this._finalizeTask('download', req.query.channel, numBytes, approvalObject);
            }
            catch(err) {
                await this._errorResponse(req, res, 500, err.message);
                // don't count against quota if failed. (TODO: should we count it partially against quota?)
                this._finalizeTask('download', req.query.channel, 0, approvalObject);
            }
        });
        this._app.post('/set/:algorithm/:hash', async (req, res) => {
            let numBytes = Number(req.headers['content-length']);
            if (isNaN(numBytes))  {
                await this._errorResponse(req, res, 500, 'Missing or invalid content-length in request header');
                return;
            }
            let approvalObject = await this._approveTask('upload', req.query.channel, req.params.algorithm, req.params.hash, numBytes, req.query.signature, req);
            if (!approvalObject.approve) {
                await this._errorResponse(req, res, 500, approvalObject.reason);
                return;
            }
            try {
                await this._apiSet(req, res);
                this._finalizeTask('upload', req.query.channel, numBytes, approvalObject);
            }
            catch(err) {
                await this._errorResponse(req, res, 500, err.message);
                // don't count against quota if failed. (TODO: should we count it partially against quota?)
                this._finalizeTask('upload', req.query.channel, 0, approvalObject);
            }
        });

        this._indexer.startIndexing();
    }
    async _apiProbe(req, res) {
        res.json({ success: true });
    }
    async _apiCheck(req, res) {
        let params = req.params;
        let query = req.query; //unused
        if (!validateHashString(params.algorithm, params.hash)) {
            await this._errorResponse(req, res, 500, `Invalid ${params.algorithm} string`);
            return;
        }
        let result = await this._caches[params.algorithm].findFileForHash(params.hash);
        res.json({ success: true, found: result.found, size: result.size });
    }
    async _apiGet(req, res) {
        let params = req.params;
        let query = req.query;
        if (!validateHashString(params.algorithm, params.hash)) {
            await this._errorResponse(req, res, 500, `Invalid ${params.algorithm} string`);
            return;
        }
        let result = await this._caches[params.algorithm].findFileForHash(params.hash);
        if ((!result.found)) {
            await this._errorResponse(req, res, 500, 'File not found.');
            return;
        }
        let X = new DownloadHandler(this._caches[params.algorithm]);
        let timer = new Date();
        try {
            await X.handleDownload(params.hash, req, res)
            let elapsed = ((new Date()) - timer) / 1000;
            console.info(`Downloaded file ${params.hash} in ${elapsed} sec.`);
        }
        catch (err) {
            console.warn(`Error downloading file ${params.hash}: ${err.message}`);
            await this._errorResponse(req, res, 500, `Error downloading file: ${err.message}`);
        }
    }
    async _apiSet(req, res) {
        /*
        Here's how to test this method:
        curl --request POST --data-binary "@file.txt" http://localhost:8081/set/sha1/c04645885a80ff25b6ec59d665034627c3a4ec45
        where file.txt is an existing file with sha1sum equal to c04... and the server is being hosted on PORT=8081
        */
        let params = req.params;
        let query = req.query;
        if (!validateHashString(params.algorithm, params.hash)) {
            await this._errorResponse(req, res, 500, `Invalid ${params.algorithm} string`);
            return;
        }
        let file_size = Number(req.headers['content-length']);
        if (isNaN(file_size))  {
            await this._errorResponse(req, res, 500, 'Missing or invalid content-length in request header');
            return;
        }
        let X = new UploadHandler(this._caches[params.algorithm]);
        let timer = new Date();
        try {
            await X.handleUpload(params.hash, file_size, req, res)
            let elapsed = ((new Date()) - timer) / 1000;
            console.info(`Uploaded file ${params.hash} in ${elapsed} sec.`);
            res.json({ success: true });
        }
        catch (err) {
            console.warn(`Error uploading file ${params.hash}: ${err.message}`);
            await this._errorResponse(req, res, 500, `Error uploading file: ${err.message}`);
        }
    }
    async _errorResponse(req, res, code, errstr) {
        console.info(`Responding with error: ${code} ${errstr}`);
        try {
            res.status(code).send(errstr);
        }
        catch(err) {
            console.warn(`Problem sending error: ${err.message}`);
        }
        await waitMsec(100);
        try {
            req.connection.destroy();
        }
        catch(err) {
            console.warn(`Problem destroying connection: ${err.message}`);
        }
    }
    async _approveTask(taskName, channel, algorithm, hash, numBytes, signature, req) {
        let approval = this._regulator.approveTask(taskName, channel, algorithm, hash, numBytes, signature, req);
        if (approval.defer) {
            console.info(`Deferring ${taskName} task`);
            while (approval.defer) {
                await waitMsec(500);
            }
            console.info(`Starting deferred ${taskName}`);
        }
        if (approval.delay) {
            await waitMsec(approval.delay);
        }
        return approval;
    }
    _finalizeTask(taskName, channel, numBytes, approvalObject) {
        return this._regulator.finalizeTask(taskName, channel, numBytes, approvalObject);
    }
    async listen(port) {
        await start_http_server(this._app, port);
    }
}

function waitMsec(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function start_http_server(app, listen_port) {
    app.port = listen_port;
    if (process.env.SSL != null ? process.env.SSL : listen_port % 1000 == 443) {
        // The port number ends with 443, so we are using https
        app.USING_HTTPS = true;
        app.protocol = 'https';
        // Look for the credentials inside the encryption directory
        // You can generate these for free using the tools of letsencrypt.org
        const options = {
            key: fs.readFileSync(__dirname + '/encryption/privkey.pem'),
            cert: fs.readFileSync(__dirname + '/encryption/fullchain.pem'),
            ca: fs.readFileSync(__dirname + '/encryption/chain.pem')
        };

        // Create the https server
        app.server = https.createServer(options, app);
    } else {
        app.protocol = 'http';
        // Create the http server and start listening
        app.server = http.createServer(app);
    }
    await app.server.listen(listen_port);
    console.info(`Server is running ${app.protocol} on port ${app.port}`);
}

function readJsonFile(filePath) {
    const txt = fs.readFileSync(filePath);
    try {
        return JSON.parse(txt);
    }
    catch (err) {
        throw new Error(`Unable to parse JSON of file: ${filePath}`);
    }
}

function mkdirIfNeeded(path) {
    if (!fs.existsSync(path)) {
        fs.mkdirSync(path);
    }
}

function validateHashString(algorithm, hash) {
    if (algorithm == 'sha1') {
        if (hash.length != 40) {
            return false;
        }
        return true;
    }
    else if (algorithm == 'md5') {
        if (hash.length != 32) {
            return false;
        }
        return true;
    }
    else {
        return false;
    }
}