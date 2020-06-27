import kachery as ka

class Explore:
    def __init__(self):
        super().__init__()

    def javascript_state_changed(self, prev_state, state):
        ka.set_config(fr='default_readonly', to='')

        self._set_status('running', 'Running Explore')

        self._set_state(
            dir_content=None,
            file_content=None
        )

        print('running', state)
        if state.get('path'):
            try:
                dir_content = ka.read_dir(state['path'])
                file_content = None
            except:
                dir_content = None
                file_content = ka.load_text(state['path'])
            self._set_state(
                dir_content=dir_content,
                file_content=file_content
            )    

        self._set_status('finished', 'Finished Explore ' + state['path'])

    def on_message(self, msg):
        ka.set_config(fr='default_readonly', to='')
        # process custom messages from JavaScript here
        # In .js file, use this.pythonInterface.sendMessage({...})
        if msg['name'] == 'loadText':
            try:
                text = ka.load_text(msg['path'])
            except:
                self._send_message(dict(
                    name='loadedText',
                    path=msg['path'],
                    text=None,
                    error='Problem loading. Perhaps this is not a text file.'
                ))
                return
            self._send_message(dict(
                name='loadedText',
                path=msg['path'],
                text=text
            ))
    
    # Send a custom message to JavaScript side
    # In .js file, use this.pythonInterface.onMessage((msg) => {...})
    def _send_message(self, msg):
        self.send_message(msg)

    # Set the python state
    def _set_state(self, **kwargs):
        self.set_state(kwargs)
    
    # Set error status with a message
    def _set_error(self, error_message):
        self._set_status('error', error_message)
    
    # Set status and a status message. Use running', 'finished', 'error'
    def _set_status(self, status, status_message=''):
        self._set_state(status=status, status_message=status_message)