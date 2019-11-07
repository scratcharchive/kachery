import fs from 'fs';
import crypto from 'crypto';

export default class KacheryTaskRegulator {
    constructor(config) {
        this._channels = {};

        if (!config.channels) {
            throw new Error(`Missing field in config: channels`);
        }
        if (config.channels.length === 0) {
            console.warn('No channels provided in config file');
        }
        for (let ch of config.channels) {
            this._channels[ch.name] = new Channel(ch);
        }
    }
    approveTask(taskName, channelName, algorithm, hash, numBytes, signature, req) {
        if (!this._channels[channelName]) {
            return { approved: false, reason: `Channel not found in config: ${channelName}` };
        }
        let channel = this._channels[channelName];
        if (!verifySignature(taskName, algorithm, hash, channel.password(), signature)) {
            return { approve: false, reason: 'incorrect or missing signature', delay: 1000 };
        }
        if (taskName === 'check') {
            return channel.approveCheckTask(req);
        }
        else if (taskName === 'download') {
            return channel.approveDownloadTask(numBytes, req);
        }
        else if (taskName === 'upload') {
            return channel.approveUploadTask(numBytes, req);
        }
        else {
            throw new Error(`Unexpected taskName in approveTask: ${taskName}`);
        }
    }
    finalizeTask(taskName, channelName, numBytes, approvalObject) {
        if (!this._channels[channelName]) {
            return { approved: false, reason: `Channel not found in config: ${channelName}` };
        }
        let channel = this._channels[channelName];
        if (taskName === 'check') {
            return channel.finalizeCheckTask(approvalObject);
        }
        else if (taskName === 'download') {
            return channel.finalizeDownloadTask(numBytes, approvalObject);
        }
        else if (taskName === 'upload') {
            return channel.finalizeUploadTask(numBytes, approvalObject);
        }
        else {
            throw new Error(`Unexpected taskName in finalizeTask: ${taskName}`);
        }
    }
}

class Channel {
    constructor(config) {
        this._password = config.password;
        this._downloadQuotas = [];
        this._uploadQuotas = [];

        for (let q of config.downloadQuotas) {
            this._downloadQuotas.push(new Quota(q));
        }
        for (let q of config.uploadQuotas) {
            this._uploadQuotas.push(new Quota(q));
        }
    }
    password() {
        return this._password;
    }
    approveCheckTask(req) {
        return { approve: true };
    }
    approveDownloadTask(numBytes, req) {
        let q = this._findRelevantDownloadQuota(numBytes);
        if (!q) {
            return { approve: false, reason: 'No relevant download quota found' };
        }
        return q.approveTask(numBytes, req);
    }
    approveUploadTask(numBytes, req) {
        let q = this._findRelevantUploadQuota(numBytes);
        if (!q) {
            return { approve: false, reason: 'No relevant upload quota found' };
        }
        return q.approveTask(numBytes, req);
    }
    finalizeCheckTask(approvalObject) {
        return;
    }
    finalizeDownloadTask(numBytes, approvalObject) {
        approvalObject.quota.finalizeTask(numBytes, approvalObject);
    }
    finalizeUploadTask(numBytes, approvalObject) {
        approvalObject.quota.finalizeTask(numBytes, approvalObject);
    }
    _findRelevantDownloadQuota(numBytes) {
        for (let q of this._downloadQuotas) {
            if (numBytes <= q.maxFileSize()) {
                return q;
            }
        }
        return null;
    }
    _findRelevantUploadQuota(numBytes) {
        for (let q of this._uploadQuotas) {
            if (numBytes <= q.maxFileSize()) {
                return q;
            }
        }
        return null;
    }
}

// Download or Upload quota
class Quota {
    constructor(config) {
        this._maxFileSize = config.maxFileSize;
        this._maxSimultaneous = config.maxSimultaneous;
        this._maxNumFilesPerDay = config.maxNumFilesPerDay;
        this._maxNumBytesPerDay = config.maxNumBytesPerDay;
        this._deferredApprovals = [];

        this._numActiveTasks = 0;
        this._newDay();
    }
    maxFileSize() {
        return this._maxFileSize;
    }
    _newDay() {
        this._currentDay = new Date();
        this._totalNumFilesToday = 0;
        this._totalNumBytesToday = 0;
        this._pendingNumFilesToday = 0;
        this._pendingNumBytesToday = 0;
    }
    approveTask(numBytes, req) {
        let timestamp = new Date();
        if (!sameDay(this._currentDay, timestamp)) {
            this._newDay();
        }
        let effectiveNumFiles = 1;
        let effectiveNumBytes = numBytes;
        if (req.method === 'HEAD') {
            // we don't count it as a download if it is a HEAD request
            effectiveNumFiles = 0;
            effectiveNumBytes = 0;
        }
        if ((req.method === 'GET') && (req.headers.range)) {
            // we only want to count the number of bytes that are actually downloaded
            effectiveNumBytes = getNumBytesFromRangeHeader(req, numBytes);
        }
        if (this._totalNumFilesToday + this._pendingNumFilesToday + 1 > this._maxNumFilesPerDay) {
            return { approve: false, reason: 'Exceeded number of files per day for this quota.' };
        }
        if (this._totalNumBytesToday + this._pendingNumBytesToday + effectiveNumBytes > this._maxNumBytesPerDay) {
            return { approve: false, reason: 'Exceeded number of bytes per day for this quota.' };
        }
        let approval = {
            approve: true,
            quota: this,
            effectiveNumBytes: effectiveNumBytes,
            effectiveNumFiles: effectiveNumFiles,
            timestamp: timestamp,
            delay: null
        };
        approval.start = () => {
            this._numActiveTasks += 1;
            this._pendingNumFilesToday += effectiveNumFiles;
            this._pendingNumBytesToday += effectiveNumBytes;
        };
        approval.checkReady = () => {
            return (this._numActiveTasks + 1 <= this._maxSimultaneous);
        }
        if (approval.checkReady()) {
            approval.start();
        }
        else {
            this._deferredApprovals.push(approval);
            approval.defer = true;
        }
        return approval;
    }
    finalizeTask(numBytes, approvalObject) {
        this._numActiveTasks -= 1;
        if (sameDay(approvalObject.timestamp, this._currentDay)) {
            this._pendingNumFilesToday -= approvalObject.effectiveNumFiles;
            this._pendingNumBytesToday -= approvalObject.effectiveNumBytes;
            this._totalNumFilesToday += approvalObject.effectiveNumFiles;
            this._totalNumBytesToday += approvalObject.effectiveNumBytes; // Note that numBytes may be different from approvalObject.numBytes
        }
        let somethingChanged = false;
        for (let i = 0; i < this._deferredApprovals.length; i++) {
            if (this._deferredApprovals[i].checkReady()) {
                this._deferredApprovals[i].start();
                this._deferredApprovals[i].defer = false;
                somethingChanged = true;
            }
        }
        if (somethingChanged) {
            let newDeferredApprovals = [];
            for (let da of this._deferredApprovals) {
                if (da.defer) {
                    newDeferredApprovals.push(da);
                }
            }
            this._deferredApprovals = newDeferredApprovals;
        }
        console.info({
            pendingNumFilesToday: this._pendingNumFilesToday,
            pendingNumBytesToday: this._pendingNumBytesToday,
            totalNumFilesToday: this._totalNumFilesToday,
            totalNumBytesToday: this._totalNumBytesToday
        });
    }
}

function getNumBytesFromRangeHeader(req, numBytes) {
    let subranges = req.range(numBytes);
    if (subranges.type != 'bytes') {
        console.warn('Range header type is not bytes. Using full size of file for quota approvals.');
        return numBytes;
    }
    let ret = 0;
    for (let sr of subranges) {
        ret += sr.end - sr.start + 1;
    }
    return ret;
}

function sha1OfObject(obj) {
    let shasum = crypto.createHash('sha1');
    shasum.update(JSON.stringify(obj));
    return shasum.digest('hex');
}

function verifySignature(name, algorithm, hash, password, signature) {
    if (process.env.KACHERY_TEST_SIGNATURE) {
        if ((signature === process.env.KACHERY_TEST_SIGNATURE)) {
            console.warn('WARNING: verified using test signature from KACHERY_TEST_SIGNATURE environment variable');
            return true;
        }
    }
    let expectedSignature = sha1OfObject({
        // keys in alphabetical order
        algorithm: algorithm,
        hash: hash,
        name: name,
        password: password
    });
    return ((signature === expectedSignature));
}

function sameDay(d1, d2) {
    return (
        (d1.getFullYear() === d2.getFullYear()) &&
        (d1.getMonth() === d2.getMonth()) &&
        (d1.getDate() === d2.getDate())
    );
}

