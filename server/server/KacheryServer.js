import express from 'express';
import https from 'https';
import http from 'http';
import Sha1Cache from './Sha1Cache.js';
import DownloadHandler from './DownloadHandler.js';
import UploadHandler from './UploadHandler.js';

export default class KacheryServer {
    constructor(storageDir, regulator) {
        this._storageDir = storageDir;
        this._regulator = regulator;
        this._sha1Cache = new Sha1Cache(this._storageDir);

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
        this._app.get('/check/sha1/:sha1', async (req, res) => {
            let approvalObject = await this._approveTask('check', req.query.channel, req.params.sha1, null, req.query.signature, req);
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
        this._app.get('/get/sha1/:sha1', async (req, res) => {
            ///////////////////////////////////////////////////////////////////////
            // First we need to do a check to determine the file size
            let params = req.params;
            if (params.sha1.length != 40) {
                await this._errorResponse(req, res, 500, 'Invalid sha1 string');
                return;
            }
            let result = await this._sha1Cache.findFileForSha1(params.sha1);
            if (!result.found) {
                await this._errorResponse(req, res, 500, 'Not found.');
                return;
            }
            let numBytes = result.size;
            ///////////////////////////////////////////////////////////////////////

            let approvalObject = await this._approveTask('download', req.query.channel, req.params.sha1, numBytes, req.query.signature, req);
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
        this._app.post('/set/sha1/:sha1', async (req, res) => {
            let numBytes = Number(req.headers['content-length']);
            if (isNaN(numBytes))  {
                await this._errorResponse(req, res, 500, 'Missing or invalid content-length in request header');
                return;
            }
            let approvalObject = await this._approveTask('upload', req.query.channel, req.params.sha1, numBytes, req.query.signature, req);
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
    }
    async _apiProbe(req, res) {
        res.json({ success: true });
    }
    async _apiCheck(req, res) {
        let params = req.params;
        let query = req.query; //unused
        if (params.sha1.length != 40) {
            await this._errorResponse(req, res, 500, 'Invalid sha1 string');
            return;
        }
        let result = await this._sha1Cache.findFileForSha1(params.sha1);
        res.json({ success: true, found: result.found, size: result.size });
    }
    async _apiGet(req, res) {
        let params = req.params;
        let query = req.query;
        if (params.sha1.length != 40) {
            await this._errorResponse(req, res, 500, 'Invalid sha1 string');
            return;
        }
        let result = await this._sha1Cache.findFileForSha1(params.sha1);
        if ((!result.found)) {
            await this._errorResponse(req, res, 500, 'File not found.');
            return;
        }
        let X = new DownloadHandler(this._sha1Cache);
        let timer = new Date();
        try {
            await X.handleDownload(params.sha1, req, res)
            let elapsed = ((new Date()) - timer) / 1000;
            console.info(`Downloaded file ${params.sha1} in ${elapsed} sec.`);
        }
        catch (err) {
            console.warn(`Error downloading file ${params.sha1}: ${err.message}`);
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
        if (params.sha1.length != 40) {
            await this._errorResponse(req, res, 500, 'Invalid sha1 string');
            return;
        }
        let file_size = Number(req.headers['content-length']);
        if (isNaN(file_size))  {
            await this._errorResponse(req, res, 500, 'Missing or invalid content-length in request header');
            return;
        }
        let X = new UploadHandler(this._sha1Cache);
        let timer = new Date();
        try {
            await X.handleUpload(params.sha1, file_size, req, res)
            let elapsed = ((new Date()) - timer) / 1000;
            console.info(`Uploaded file ${params.sha1} in ${elapsed} sec.`);
            res.json({ success: true });
        }
        catch (err) {
            console.warn(`Error uploading file ${params.sha1}: ${err.message}`);
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
    async _approveTask(taskName, channel, sha1, numBytes, signature, req) {
        let approval = this._regulator.approveTask(taskName, channel, sha1, numBytes, signature, req);
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