import React, { Component } from "react";

import Highlight from "react-highlight.js";
import 'highlight.js/styles/railscasts.css';

export default class FileContentView extends Component {
    constructor(props) {
        super(props);
        this.state = {
            fileContent: null,
            fileContentStatus: 'not-loading',
            contentVisible: false
        };
    }

    async componentDidMount() {
        this._checkSize();
        if (this.state.contentVisible) {
            await this.updateContent();
        }
    }

    async componentDidUpdate(prevProps, prevState) {
        if ((prevProps.path !== this.props.path) || (prevProps.size !== this.props.size) || (this.state.contentVisible != prevState.contentVisible)) {
            this._checkSize();
            if (this.state.contentVisible) {
                await this.updateContent();
            }
        }
    }

    _checkSize() {
        const { size } = this.props;
        if (size) {
            if (size < 10000) {
                if (!this.state.contentVisible) {
                    this.setState({
                        contentVisible: true
                    });
                }
            }
        }
    }

    async updateContent() {
        const { path, size } = this.props;
        const max_size = 500000;

        this.setState({
            fileContentStatus: 'not-loading',
            fileContent: null
        });

        if (size > max_size) {
            this.setState({
                fileContentStatus: `Cannot load file content, size exceeds maximum (${size} > ${max_size})`
            });
            return;
        }
        if (this.state.contentVisible) {
            this.setState({
                fileContentStatus: 'loading'
            });
            let txt0 = await this.props.kacheryManager.loadText(path);
            if (txt0) {
                this.setState({
                    fileContentStatus: 'loaded',
                    fileContent: txt0
                });
            }
            else {
                this.setState({
                    fileContentStatus: 'failed'
                });
            }
        }
    }

    getContentElement() {
        if (this.state.fileContentStatus === 'loading') {
            return <div>Loading content...</div>;
        }
        else if (this.state.fileContentStatus === 'failed') {
            return <div>Failed to load content</div>;
        }
        else if (this.state.fileContentStatus === 'loaded') {
            let language = determineLanguageFromFilePath(this.props.path);
            console.log(`language for ${this.props.path} is ${language}`);
            return <Highlight language={language}>
                {this.state.fileContent}
            </Highlight>
        }
        else {
            return <div>{this.state.fileContentStatus}</div>;
        }
    }

    handleShowFileContent = () => {
        this.setState({
            contentVisible: false
        });
    }

    render() {
        if (this.state.contentVisible) {
            return <div>
                <div style={{ overflow: 'auto', height: '300px' }}>
                    {this.getContentElement()}
                </div>
                <button onClick={() => { this.setState({ contentVisible: false }) }}>Hide file content</button>
            </div>
        }
        else {
            return <button onClick={() => { this.setState({ contentVisible: true }) }}>Show file content</button>;
        }
    }
}

function determineLanguageFromFilePath(path) {
    let map = {
        '.js': 'javascript',
        '.json': 'json',
        '.py': 'python',
        '.html': 'html',
        '.md': 'markdown'
    };
    for (let key in map) {
        if (path.endsWith(key)) {
            return map[key];
        }
    }
    return '';
}

// async function loadText(path, opts) {
//     let response;
//     try {
//         response = await axios.get(`/api/loadText?path=${encodeURIComponent(path)}`);
//     }
//     catch (err) {
//         console.error(err);
//         return null;
//     }
//     let rr = response.data;
//     if (rr.success) {
//         return rr.text;
//     }
//     else return null;
// }
