import React, { Component } from 'react';
import Tree from './Tree';
import PathBar from './PathBar';
import ItemView from './ItemView';
import { Grid } from "@material-ui/core";
import * as itemViewPlugins from "./itemviewplugins";

export default class ExploreWidget extends Component {
    constructor(props) {
        super(props);
        this.state = {
            rootNode: null,
            selectedNodePath: null,
            selectedItem: null
        }
        this.nodeCreator = new NodeCreator();
    }
    
    componentDidMount() {
        this._updateRootNode();
    }
    componentDidUpdate(prevProps) {
        if ((this.props.dirContent !== prevProps.dirContent) || (this.props.fileContent !== prevProps.fileContent) || (this.props.path !== prevProps.path)) {
            this._updateRootNode();
        }
    }
    _updateRootNode() {
        this.setState({
            rootNode: null,
            selectedNodePath: null,
            selectedItem: null
        });
        if (this.props.dirContent) {
            let rootNode = this.nodeCreator.createDirNode(this.props.dirContent, '', this.props.path);
            this.setState({
                rootNode: rootNode
            });
        }
        else if (this.props.fileContent) {
            let obj = null;
            try {
                obj = JSON.parse(this.props.fileContent);
            }
            catch(err) {
                console.warn('Unable to parse JSON file content.');
            }
            if (obj) {
                let rootNode = this.nodeCreator.createObjectNode(obj);
                this.setState({
                    rootNode: rootNode
                });
            }
        }
        
    }
    _handleNodeSelected = (node) => {
        if (node) {
            let item = {
                type: node.type,
                name: node.name,
                path: node.path,
                data: JSON.parse(JSON.stringify(node.data))
            };
            this.setState({
                selectedNodePath: node ? node.path : null,
                selectedItem: item
            });
        }
        else {
            this.setState({
                selectedNodePath: null,
                selectedItem: null
            });
        }
    }
    _handlePathChanged = (path) => {
        this.props.onPathChanged && this.props.onPathChanged(path);
    }
    _handleBackButton = () => {
        this.props.onBackButton && this.props.onBackButton();
    }
    render() {
        const { rootNode, selectedItem } = this.state;

        const pathBar = (
            <PathBar
                path={this.props.path}
                pathHistory={this.props.pathHistory}
                onPathChanged={this._handlePathChanged}
                onBackButton={this._handleBackButton}
            />
        );

        let tree = (
            <Tree
                rootNode={rootNode}
                selectedNodePath={this.state.selectedNodePath}
                onNodeSelected={this._handleNodeSelected}
            />
        );

        let itemView = (
            <ItemView
                item={selectedItem}
                viewPlugins={Object.values(itemViewPlugins)}
                kacheryManager={this.props.kacheryManager}
                onOpenPath={this._handlePathChanged}
                onSelectItem={null}
            />
        );

        return (
            <Grid container>
                <Grid item xs={12}>
                    {pathBar}
                </Grid>
                <Grid item xs={6}>
                    {tree}
                </Grid>
                <Grid item xs={6}>
                    {itemView}
                </Grid>
            </Grid>
        );
    }
}

class NodeCreator {
    createObjectNode(obj, name, basepath, part_of_list) {
        const max_array_children = 20;
        let childNodes = [];
        let path0 = this.joinPaths(basepath, name, '.', part_of_list);
        let type0 = 'object';
        if (Array.isArray(obj)) {
            childNodes = this.createArrayHierarchyChildNodes(obj, max_array_children, 0, obj.length, path0);
        }
        else {
            let keys = Object.keys(obj);
            keys.sort();
            for (let key of keys) {
                let val = obj[key];
                if ((val) && (typeof (val) == 'object')) {
                    childNodes.push(this.createObjectNode(val, key, path0));
                }
                else {
                    childNodes.push(this.createValueNode(val, key, path0));
                }
            }
        }
        return {
            type: type0,
            name: name,
            childNodes: childNodes,
            path: path0,
            data: {
                object: obj
            }
        }
    }

    createArrayHierarchyChildNodes(X, max_array_children, i1, i2, path0) {
        let childNodes = [];
        if (i2 - i1 <= max_array_children) {
            for (let ii = i1; ii < i2; ii++) {
                let val = X[ii];
                if ((val) && (typeof (val) == 'object')) {
                    childNodes.push(this.createObjectNode(val, '' + ii, path0, true));
                }
                else {
                    childNodes.push(this.createValueNode(val, '' + ii, path0, true));
                }
            }
        }
        else {
            let stride = 1;
            while ((i2 - i1) / stride > max_array_children / 2) {
                stride = stride * 10;
            }
            for (let jj = i1; jj < i2; jj += stride) {
                let jj2 = jj + stride;
                if (jj2 >= i2) jj2 = i2;
                childNodes.push({
                    type: 'array-parent',
                    name: `${jj} - ${jj2 - 1}`,
                    path: path0 + `[${jj}-${jj2 - 1}]`,
                    data: {},
                    childNodes: this.createArrayHierarchyChildNodes(X, max_array_children, jj, jj2, path0),
                });
            }
        }
        return childNodes;

    }

    createValueNode(val, name, basepath) {
        let path0 = this.joinPaths(basepath, name, '.');
        return {
            type: 'value',
            name: name,
            path: path0,
            data: {
                value: val
            }
        };
    }

    createDirNode(X, name, basepath) {
        let childNodes = [];
        let path0 = this.joinPaths(basepath, name, '/');
        let dnames = Object.keys(X.dirs);
        dnames.sort();
        for (let dname of dnames) {
            childNodes.push(this.createDirNode(X.dirs[dname], dname, path0));
        }
        let fnames = Object.keys(X.files);
        fnames.sort();
        for (let fname of fnames) {
            childNodes.push(this.createFileNode(X.files[fname], fname, path0));
        }
        return {
            type: 'dir',
            name: name,
            path: path0,
            data: {
                dir: X
            },
            childNodes: childNodes
        };
    }

    createFileNode(X, name, basepath) {
        let path0 = this.joinPaths(basepath, name, '/');
        return {
            type: 'file',
            name: name,
            path: path0,
            data: {
                file: X
            },
            childNodes: []
        };
    }

    joinPaths(path1, path2, sep, part_of_list) {
        if (!path2) return path1;
        if (!path1) return path2;
        if (part_of_list) {
            return `${path1}.${path2}`;
        }
        else {
            return `${path1}${sep}${path2}`;
        }
    }
}