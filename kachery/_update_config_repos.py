import os
import json
import time
import stat
import shutil
import subprocess
import traceback
from .filelock import FileLock

def _file_age_sec(pathname):
    return time.time() - os.stat(pathname)[stat.ST_MTIME]

def _clone_git_repo_branch(*, url, dest_path, branch):
    cmd = f"git clone {url} --branch {branch} --single-branch {dest_path}"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    assert process.returncode == 0, f'Error cloning git repo with command: {cmd}'

def _pull_git_repo(*, path):
    process = subprocess.Popen(f"git pull", shell=True, cwd=path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    assert process.returncode == 0, 'Error pulling git repo'

def _update_config_repo_branch(*, config_repos_path, name, repo_url, branch, folder, recursive=True) -> dict:
    config_repo_path = config_repos_path + '/' + name
    if not os.path.exists(config_repo_path):
        os.mkdir(config_repo_path)
    config_fname = config_repo_path + '/config.json'
    try:
        with open(config_fname, 'r') as f:
            config0 = json.load(f)
    except:
        config0 = None
    if config0 is not None and config0['url'] == repo_url and config0['branch'] == branch and config0['folder'] == folder and os.path.exists(config_repo_path + '/repo'):
        if _file_age_sec(config_fname) < 60:
            return config0
        else:
            try:
                _pull_git_repo(path=config_repo_path + '/repo')
            except:
                print(f'WARNING: unable to pull git repo: {config_repo_path}/repo')
    else:
        shutil.rmtree(config_repo_path)
        os.mkdir(config_repo_path)
        try:
            _clone_git_repo_branch(url=repo_url, dest_path=config_repo_path + '/repo', branch=branch)
        except:
            traceback.print_exc()
            raise Exception(f'Unable to clone git repo: {repo_url}')
    siblings = []
    siblings_folder = os.path.join(config_repo_path, 'repo', folder, 'siblings')
    if os.path.exists(siblings_folder):
        list0 = os.listdir(siblings_folder)
        for fname in list0:
            if fname.endswith('.json'):
                with open(os.path.join(siblings_folder, fname), 'r') as f:
                    sibling_config = json.load(f)
                siblings.append(dict(
                    name=sibling_config['name'],
                    repo_url=sibling_config['repo_url'],
                    branch=sibling_config['branch'],
                    folder=sibling_config['folder']
                ))
    servers = []
    servers_folder = os.path.join(config_repo_path, 'repo', folder, 'servers')
    if os.path.exists(servers_folder):
        list0 = os.listdir(servers_folder)
        for fname in list0:
            if fname.endswith('.json'):
                with open(os.path.join(servers_folder, fname), 'r') as f:
                    server_config = json.load(f)
                servers.append(server_config)
    if recursive:
        for sibling in siblings:
            x = _update_config_repo_branch(
                config_repos_path=config_repos_path,
                name=sibling['name'],
                repo_url=sibling['repo_url'],
                branch=sibling['branch'],
                folder=sibling['folder']
            )
            for server0 in x['servers']:
                if server0['name'] not in [s['name'] for s in servers]:
                    servers.append(server0)
                else:
                    print(f'WARNING: duplicate server name in config: {server0["name"]}')
    config0 = dict(
        url=repo_url,
        branch=branch,
        folder=folder,
        siblings=siblings,
        servers=servers
    )
    with open(config_fname, 'w') as f:
        json.dump(config0, f, indent=4)
    return config0

def _update_config_repos(config_repos_path) -> dict:
    if not os.path.exists(config_repos_path):
        try:
            os.mkdir(config_repos_path)
        except:
            if not os.path.exists(config_repos_path):
                raise Exception(f'Problem creating directory: {config_repos_path}')
    with FileLock(config_repos_path + '/update.lock', exclusive=True):
        return _update_config_repo_branch(
            config_repos_path=config_repos_path,
            name='main',
            repo_url='https://github.com/flatironinstitute/kachery',
            branch='config',
            folder='config_2020a'
        )