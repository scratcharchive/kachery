import React, { Component } from 'react';
import { PythonInterface } from 'reactopya';
import ExploreWidget from './ExploreWidget';
const config = require('./Explore.json');

export default class Explore extends Component {
    static title = 'Explore kachery directories and files'
    static reactopyaConfig = config
    constructor(props) {
        super(props);
        this.state = {
            // javascript state
            path: null,
            
            // python state
            dir_content: null,
            file_content: null,
            status: '',
            status_message: '',

            // other
            pathHistory: []
        }
    }
    componentDidMount() {
        this.pythonInterface = new PythonInterface(this, config);
        this.kacheryManager = new KacheryManager(this);
        this.pythonInterface.start();
        this.setState({
            status: 'started',
            status_message: 'Starting python backend'
        });
        // Use this.pythonInterface.setState(...) to pass data to the python backend
        this.pythonInterface.setState({
            path: this.props.path,
        });
    }
    componentDidUpdate() {

    }
    componentWillUnmount() {
        if (this.pythonInterface) {
            this.pythonInterface.stop();
        }
    }
    _handlePathChanged = (path) => {
        this.setState({
            pathHistory: [...this.state.pathHistory, this.state.path]
        });
        this.pythonInterface.setState({
            path: path
        });
    }
    _handleBackButton = () => {
        let { pathHistory } = this.state;
        if (pathHistory.length === 0) return;
        
        let path0 = pathHistory.pop();

        this.setState({
            pathHistory: pathHistory
        });
        this.pythonInterface.setState({
            path: path0
        });
    }
    render() {
        const { path, dir_content, file_content, pathHistory } = this.state;
        return (
            <ExploreWidget
                path={path || this.props.path}
                dirContent={dir_content}
                fileContent={file_content}
                pathHistory={pathHistory}
                onPathChanged={this._handlePathChanged}
                onBackButton={this._handleBackButton}
                kacheryManager={this.kacheryManager}
            />
        );
    }
}

class KacheryManager {
    constructor(component) {
        this._component = component;
        this._retrievedText = {};
        this._component.pythonInterface.onMessage((msg) => {
            if (msg.name === 'loadedText') {
                this._retrievedText[msg.path] = {
                    text: msg.text,
                    error: msg.error
                };
            }
        });
    }
    async loadText(path) {
        this._component.pythonInterface.sendMessage({
            name: 'loadText',
            path: path
        });
        while (!this._retrievedText[path]) {
            await waitMsec(500);
        }
        return this._retrievedText[path].text;
    }
}

function waitMsec(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

class RespectStatus extends Component {
    state = {}
    render() {
        switch (this.props.status) {
            case 'started':
                return <div>Started: {this.props.status_message}</div>
            case 'running':
                return <div>{this.props.status_message}</div>
            case 'error':
                return <div>Error: {this.props.status_message}</div>
            case 'finished':
                return this.props.children;
            default:
                return <div>Unknown status: {this.props.status}</div>
        }
    }
}